package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fs.misc.LazyIterators;
import com.fs.misc.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CrossAZBlockPlacementPolicy extends BlockPlacementPolicy {

    public static final Log LOGGER = LogFactory.getLog(CrossAZBlockPlacementPolicy.class);

    public static class Estimator {

        public Map<Integer, NavigableSet<Node>> hierarchy(NavigableSet<Node> nodes) {
            return nodes.stream()
                    .flatMap((node) -> {
                        Iterator<Node> iterator = LazyIterators.generate(
                                node,
                                Optional::ofNullable,
                                (context, new_value) -> context.getParent()
                        );
                        return LazyIterators.stream(iterator);
                    })
                    .filter((node) -> node.getLevel() != 0)
                    .collect(Collectors.groupingByConcurrent(
                            Node::getLevel,
                            CrossAZBlockPlacementPolicy.nodeSetCollector())
                    );
        }

        public Map<Node, NavigableSet<Node>> clusterByParent(NavigableSet<Node> nodes) {
            return nodes.stream()
                    .collect(
                            Collectors.groupingBy(
                                    Node::getParent,
                                    CrossAZBlockPlacementPolicy.nodeSetCollector()
                            )
                    );
        }

        public boolean isOverReplication(NavigableSet<Node> nodes, int require_replica) {
            switch (require_replica) {
                case 0:
                    // no rack needed
                    return true;
                case 1:
                    // not empty rack is sufficient
                    return !nodes.isEmpty();
            }

            Map<Integer, NavigableSet<Node>> hierarchy = hierarchy(nodes);

            boolean overloaded = hierarchy.values().stream()
                    .anyMatch((same_level) -> {
                        // at least two nodes require for each level
                        if (same_level.size() < 2) {
                            return false;
                        }

                        // cluster by parent
                        Map<Node, NavigableSet<Node>> cluster_by_parent = clusterByParent(same_level);

                        // calculate expected load
                        int total = same_level.size();
                        int cluster = cluster_by_parent.size();
                        int average_load = total / cluster;
                        if (total % cluster != 0) {
                            // perfect divided
                            average_load += 1;
                        }

                        final int load_limit = average_load;
                        // find overload?
                        boolean overload = cluster_by_parent.values().stream()
                                .map((children) -> children.size() <= load_limit)
                                .anyMatch((satisfied) -> !satisfied);

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(String.format(
                                    "child overloaded:%b , child group:{%s}, expected load:%s",
                                    overload,
                                    cluster_by_parent.values().stream()
                                            .map((children) -> String.format(
                                                    "%b,[%s]",
                                                    children.size() > load_limit,
                                                    children.stream()
                                                            .map((child) -> String.format(
                                                                    "(%s)",
                                                                    child
                                                            ))
                                                            .collect(Collectors.joining(","))
                                                    )
                                            )
                                            .collect(Collectors.joining(",")),
                                    load_limit
                            ));
                        }

                        return overload;
                    });

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format(
                        "racks:[%s] require_replica:%d over replication:%b",
                        hierarchy.entrySet().stream()
                                .map((entry) -> {
                                    return "("
                                            + entry.getKey() + " = "
                                            + entry.getValue().stream().map(Node::toString)
                                            .collect(Collectors.joining(","))
                                            + ")";
                                })
                                .collect(Collectors.joining(",")),
                        require_replica,
                        overloaded
                ));
            }

            return overloaded;
        }
    }

    protected static class CrossAZBlockBlockPlacementStatus implements BlockPlacementStatus {

        protected final boolean ok;
        protected final Supplier<String> reason;

        public CrossAZBlockBlockPlacementStatus() {
            this.ok = true;
            this.reason = () -> "";
        }

        public CrossAZBlockBlockPlacementStatus(Promise.PromiseSupplier<String> reason) {
            this.ok = false;
            this.reason = reason;
        }

        @Override
        public boolean isPlacementPolicySatisfied() {
            return ok;
        }

        @Override
        public String getErrorDescription() {
            return reason.get();
        }

        @Override
        public String toString() {
            return "satisfied:" + ok + " reason:" + reason.get();
        }
    }

    protected static final CrossAZBlockBlockPlacementStatus PLACEMENT_OK = new CrossAZBlockBlockPlacementStatus();

    protected static final Comparator<Node> NODE_COMPARATOR = Comparator
            .comparingInt(Node::getLevel)
            .thenComparing(Node::getName);

    protected static final Comparator<DatanodeStorageInfo> STORAGE_COMPARATOR = Comparator
            .<DatanodeStorageInfo>comparingInt((storage) -> storage.getDatanodeDescriptor().getLevel())
            .thenComparing((storage) -> storage.getDatanodeDescriptor().getName())
            .thenComparing(DatanodeStorageInfo::getState)
            .thenComparing(DatanodeStorageInfo::getStorageType)
            .thenComparing(DatanodeStorageInfo::getStorageID);

    protected static <T extends Node> NavigableSet<T> newNodeSet() {
        return new TreeSet<>(NODE_COMPARATOR);
    }

    protected static NavigableSet<DatanodeStorageInfo> newStorageSet() {
        return new TreeSet<>(STORAGE_COMPARATOR);
    }

    protected static <T extends Node> Collector<T, ?, NavigableSet<T>> nodeSetCollector() {
        return Collectors.toCollection(CrossAZBlockPlacementPolicy::newNodeSet);
    }

    protected static Collector<DatanodeStorageInfo, ?, NavigableSet<DatanodeStorageInfo>> storageSetCollector() {
        return Collectors.toCollection(CrossAZBlockPlacementPolicy::newStorageSet);
    }

    protected Configuration configuration;
    protected FSClusterStats stats;
    protected NetworkTopology topology;
    protected Host2NodesMap mapping;
    protected Estimator estimator;

    @Override
    public DatanodeStorageInfo[] chooseTarget(
            String srcPath,
            int numOfReplicas,
            Node writer,
            List<DatanodeStorageInfo> chosen,
            boolean returnChosenNodes,
            Set<Node> excludedNodes,
            long blocksize,
            BlockStoragePolicy storagePolicy) {
        //TODO
        return new DatanodeStorageInfo[0];
    }

    @Override
    public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] datanodes, int require_replica) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format(
                    "verify for datanodes:[%s], require_replica:%s",
                    Arrays.stream(Optional.ofNullable(datanodes).orElseGet(() -> new DatanodeInfo[0]))
                            .map((datanode) -> String.format(
                                    "(%s,%s)",
                                    datanode.toString(),
                                    datanode.getNetworkLocation()
                            ))
                            .collect(Collectors.joining(","))
                    ,
                    require_replica
            ));
        }

        if (require_replica <= 0) {
            return PLACEMENT_OK;
        } else if (datanodes == null) {
            return new CrossAZBlockBlockPlacementStatus(() -> "no datanode for placement");
        }

        NavigableSet<DatanodeInfo> deduplicated = Arrays.stream(datanodes).collect(CrossAZBlockPlacementPolicy.nodeSetCollector());
        int selected = deduplicated.size();
        // not enough replica
        if (selected < require_replica) {
            return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                    "not enough locations, got:%s datanodes:[%s] for replication:%s",
                    selected,
                    deduplicated.stream()
                            .map(DatanodeInfo::toString)
                            .collect(Collectors.joining(",")),
                    require_replica
            ));
        }

        // 1. calculate expected level
        int replica_level = Integer.highestOneBit(require_replica);

        // 2. figure out topologys
        Map<Integer, NavigableSet<Node>> full_hierarchy = hierarchy(this.topology.getLeaves(NodeBase.ROOT));
        Map<Integer, NavigableSet<Node>> node_hierarchy = hierarchy(new ArrayList<>(deduplicated));

        // 3. adjust expected_level
        int expected_level = Math.min(
                replica_level,
                full_hierarchy.keySet().stream()
                        .max(Integer::compareTo)
                        .orElse(1)
        );

        // 4. check each level
        for (int level = 0; level <= expected_level; level++) {
            int current_level = level;
            int full_replica_for_level = full_hierarchy.get(current_level).size();
            int selection = node_hierarchy.get(current_level).size();

            // current level nodes are not enough for hold all replica
            if (full_replica_for_level < require_replica) {
                // each nodes at this level should had at least one replica
                // as well as selection fot this level should equal to max_replica_for_level
                if (full_replica_for_level == selection) {
                    // satisfied, examine next
                    continue;
                }

                // not balanced
                return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                        "require replica:%d, at level:%d full replica:%d == selection:%d, but fail to meet it",
                        require_replica,
                        current_level,
                        full_replica_for_level,
                        selection
                ));
            }

            // or current level can hold all replica.
            // each node at this level should had at most one replica.
            Map<Node, NavigableSet<Node>> groupby_parent = deduplicated.stream()
                    .collect(Collectors.groupingBy(
                            (node) -> resolveToLevel(node, current_level).orElse(null),
                            CrossAZBlockPlacementPolicy.nodeSetCollector()
                    ));

            for (Map.Entry<Node, NavigableSet<Node>> entry : groupby_parent.entrySet()) {
                if (entry.getValue().size() != 1) {
                    return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                            "node at current level:%s exhaust, over replicated for node:%s with:[%s]",
                            current_level,
                            entry.getKey(),
                            entry.getValue().stream()
                                    .map((node) -> String.format(
                                            "(%s)",
                                            node
                                    ))
                                    .collect(Collectors.joining(","))
                    ));
                }
            }
        }

        // find max level
        return PLACEMENT_OK;
    }

    @Override
    public List<DatanodeStorageInfo> chooseReplicasToDelete(
            Collection<DatanodeStorageInfo> candidates,
            int expected_replica,
            List<StorageType> excessTypes,
            DatanodeDescriptor addedNode,
            DatanodeDescriptor delNodeHint) {
        // deduplicate
        NavigableSet<DatanodeStorageInfo> storages = newStorageSet();
        storages.addAll(candidates);

        // replica is not enough, surely no eviction
        int to_evict = storages.size() - expected_replica;
        if (to_evict <= 0) {
            return Collections.emptyList();
        }

        // collect datanodes
        Map<DatanodeDescriptor, NavigableSet<DatanodeStorageInfo>> cluster_storage = clusterStorages(storages);

        // is placement satisfied?
        if (!this.verifyBlockPlacement(
                cluster_storage.keySet().toArray(new DatanodeInfo[0]), expected_replica)
                .isPlacementPolicySatisfied()) {
            // remove nothing
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format(
                        "placement not over replication,current:[%s] , require replica:%s",
                        candidates.stream().map(this::storageStringify)
                                .collect(Collectors.joining(",")),
                        expected_replica
                ));
            }
            return Collections.emptyList();
        }

        // placement ok, collect eviction candidates
        NavigableSet<DatanodeStorageInfo> eviction_candidates = newStorageSet();

        // 1. calculate depth
        int max_level = storages.stream().map(DatanodeStorageInfo::getDatanodeDescriptor)
                .map(DatanodeDescriptor::getLevel)
                .max(Integer::compareTo)
                .orElse(1);

        // prune each level
        for (int level = max_level; level > 0; level--) {
            eviction_candidates.addAll(pruneAtLevel(storages, level));
            if (eviction_candidates.size() >= to_evict) {
                break;
            }
        }

        // build storage type lookup
        Set<StorageType> storage_types = new HashSet<>(excessTypes);

        // no preferences
        return eviction_candidates.stream()
                .sorted(Comparator.<DatanodeStorageInfo>comparingInt((node) ->
                        // fail nodes first
                        node.getState() == DatanodeStorage.State.FAILED ? 0 : 1)
                        // exceed typ first
                        .thenComparing((node) -> storage_types.contains(node.getStorageType()) ? 0 : 1)
                        // respect delete hint
                        .thenComparing((node) -> delNodeHint == null ? 0 : node.getDatanodeDescriptor().compareTo(delNodeHint) == 0 ? 0 : 1)
                        // respect added hint
                        .thenComparing((node) -> addedNode == null ? 0 : node.getDatanodeDescriptor().compareTo(addedNode) == 0 ? 1 : 0)
                        // heavy used first, note the minus sign
                        .thenComparing((node) -> -node.getDfsUsed())
                )
                .limit(to_evict)
                .collect(Collectors.toList());
    }

    protected NavigableSet<Node> collectNodes() {
        return this.topology.getLeaves(NodeBase.ROOT).stream()
                .collect(CrossAZBlockPlacementPolicy.nodeSetCollector());
    }

    protected Map<Integer, NavigableSet<Node>> hierarchy(Collection<Node> nodes) {
        return nodes.stream()
                .flatMap((node) -> {
                    Iterator<Node> iterator = LazyIterators.generate(
                            node,
                            Optional::ofNullable,
                            (context, new_value) -> context.getParent()
                    );
                    return LazyIterators.stream(iterator);
                })
                .collect(Collectors.groupingBy(
                        Node::getLevel,
                        CrossAZBlockPlacementPolicy.nodeSetCollector())
                );
    }

    protected Map<DatanodeDescriptor, NavigableSet<DatanodeStorageInfo>> clusterStorages(
            NavigableSet<DatanodeStorageInfo> storages) {
        return storages.stream()
                .collect(
                        Collectors.groupingBy(
                                DatanodeStorageInfo::getDatanodeDescriptor,
                                Collectors.toCollection(CrossAZBlockPlacementPolicy::newStorageSet)
                        )
                );
    }

    protected Optional<Node> resolveToLevel(Node node, int level) {
        Node resolved = node;
        while (resolved.getLevel() > level) {
            resolved = resolved.getParent();
        }

        if (resolved.getLevel() != level) {
            // level is deeper that node`s natural level depth.
            // return self
            return Optional.empty();
        }

        return Optional.of(resolved);
    }

    protected String storageStringify(DatanodeStorageInfo storage) {
        return String.format(
                "(%s,%s,%s:%s,%s)",
                storage.getDatanodeDescriptor().getNetworkLocation(),
                storage.getDatanodeDescriptor().getLevel(),
                storage.getDatanodeDescriptor().getIpAddr(),
                storage.getDatanodeDescriptor().getIpcPort(),
                storage.getStorageID()
        );
    }

    protected NavigableSet<DatanodeStorageInfo> pruneAtLevel(NavigableSet<DatanodeStorageInfo> storages, int level) {
        return storages.stream()
                // nodes should deeper
                .filter((storage) -> storage.getDatanodeDescriptor().getLevel() >= level)
                // cluster by target level parent
                .collect(Collectors.groupingBy(
                        (storage) -> resolveToLevel(storage.getDatanodeDescriptor(), level - 1),
                        CrossAZBlockPlacementPolicy.storageSetCollector())
                )
                .entrySet().stream()
                .peek((entry) -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format(
                                "clustered node parent:(%s) size:%d children:[%s]",
                                entry.getKey().map((parent) -> String.format(
                                        "%s,%s,%s",
                                        parent.getNetworkLocation(),
                                        parent.getLevel(),
                                        parent.getName()
                                ))
                                        .orElse("root,1"),
                                entry.getValue().size(),
                                entry.getValue().stream()
                                        .map((node) -> String.format(
                                                "(%s, %s, %s)",
                                                node.getDatanodeDescriptor().getNetworkLocation(),
                                                node.getDatanodeDescriptor().getLevel(),
                                                node.getDatanodeDescriptor().getDatanodeUuid()
                                        ))
                                        .collect(Collectors.joining(","))
                        ));
                    }
                })
                // filter overloaded
                .filter((entry) -> entry.getValue().size() > 2)
                .filter((entry) -> entry.getKey().isPresent())
                .flatMap((entry) -> {
                    // find ascensor
                    Node parent = entry.getKey().get();

                    // find cluster
                    NavigableSet<DatanodeStorageInfo> cluster = entry.getValue();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format(
                                "for parent(%s,%s) , before prune:[%s]",
                                parent,
                                parent.getLevel(),
                                cluster.stream().map(this::storageStringify).collect(Collectors.joining(","))
                        ));
                    }

                    // 1. remove storage located at same node.
                    // this is necessary for datanode level,
                    // since storage has no level.
                    // one can move this out ,
                    // but keeping here improve bug/algo change immutability.
                    NavigableSet<DatanodeStorageInfo> same_node_eviction = cluster.stream()
                            .collect(Collectors.groupingBy(DatanodeStorageInfo::getDatanodeDescriptor))
                            .values().stream()
                            .filter((same_node) -> same_node.size() > 1)
                            .flatMap((same_node) -> same_node.subList(1, same_node.size()).stream())
                            .collect(Collectors.toCollection(
                                    CrossAZBlockPlacementPolicy::newStorageSet
                            ));
                    cluster.removeAll(same_node_eviction);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format(
                                "prune on same datanodes:[%s]",
                                same_node_eviction.stream()
                                        .map(this::storageStringify).collect(Collectors.joining(","))
                        ));
                    }

                    // 2. cluster by direct child
                    Map<Optional<Node>, NavigableSet<DatanodeStorageInfo>> cluster_by_direct_child =
                            cluster.stream()
                                    .collect(Collectors.groupingBy(
                                            (storage) -> resolveToLevel(
                                                    storage.getDatanodeDescriptor(),
                                                    parent.getLevel() + 1
                                            ),
                                            CrossAZBlockPlacementPolicy.storageSetCollector()
                                    ));
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format(
                                "level (%s,%s): child groups:[%s]",
                                parent.getNetworkLocation(),
                                parent.getLevel(),
                                cluster_by_direct_child.keySet().stream()
                                        .map(datanodeStorageInfos -> String.format(
                                                "%s",
                                                datanodeStorageInfos.map(Node::getNetworkLocation)
                                                        .orElse("root")
                                        ))
                                        .collect(Collectors.joining(","))
                        ));
                    }

                    // 3. calculate keep child.
                    // least first
                    NavigableSet<DatanodeStorageInfo> keep = cluster_by_direct_child.entrySet().stream()
                            .sorted(Comparator.comparing((pair) -> entry.getValue().size()))
                            .limit(2)
                            .map(Map.Entry::getValue)
                            .flatMap(Collection::stream)
                            .collect(CrossAZBlockPlacementPolicy.storageSetCollector());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format(
                                "keep candidate: %d [%s]",
                                keep.size(),
                                keep.stream()
                                        .map(this::storageStringify)
                                        .collect(Collectors.joining(","))
                        ));
                    }

                    // 4.  calculate removed child
                    cluster.removeAll(keep);
                    NavigableSet<DatanodeStorageInfo> remove_by_child_selection = cluster;
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format(
                                "remove by child selection: %d [%s]",
                                remove_by_child_selection.size(),
                                remove_by_child_selection.stream()
                                        .map(this::storageStringify)
                                        .collect(Collectors.joining(","))
                        ));
                    }

                    // 5. may be prune deeper
                    int current_level = parent.getLevel() + 1;
                    int deep_prune_level = current_level + 1;
                    NavigableSet<DatanodeStorageInfo> deep_pruned = pruneAtLevel(keep, deep_prune_level);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format(
                                "deep prune: %d :[%s]",
                                deep_pruned.size(),
                                deep_pruned.stream()
                                        .map(this::storageStringify)
                                        .collect(Collectors.joining(","))
                        ));
                    }

                    // 6. merge results
                    return Stream.of(
                            same_node_eviction,
                            remove_by_child_selection,
                            deep_pruned
                    ).flatMap(Collection::stream);
                })
                .collect(CrossAZBlockPlacementPolicy.storageSetCollector());
    }

    @Override
    protected void initialize(Configuration configuration, FSClusterStats stats, NetworkTopology topology,
                              Host2NodesMap mapping) {
        this.configuration = configuration;
        this.stats = stats;
        this.topology = topology;
        this.mapping = mapping;
        this.estimator = new Estimator();
    }
}
