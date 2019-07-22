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
import java.util.concurrent.ConcurrentSkipListSet;
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

        public boolean isOptimal(NavigableSet<Node> nodes, int require_replica) {
            switch (require_replica) {
                case 0:
                    // no rack needed
                    return true;
                case 1:
                    // not empty rack is sufficient
                    return !nodes.isEmpty();
            }

            Map<Integer, NavigableSet<Node>> hierarchy = hierarchy(nodes);

            boolean not_satisfied = hierarchy.values().stream()
                    .map((same_level) -> {
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

                        return !overload;
                    }).anyMatch((satisfied) -> !satisfied);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("racks:["
                        + hierarchy.entrySet().stream()
                        .map((entry) -> {
                            return "("
                                    + entry.getKey() + " = "
                                    + entry.getValue().stream().map(Node::toString)
                                    .collect(Collectors.joining(","))
                                    + ")";
                        })
                        .collect(Collectors.joining(","))
                        + "]"
                        + " require_replica:" + require_replica
                        + " optimal:" + !not_satisfied
                );
            }

            return !not_satisfied;
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
            LOGGER.debug("veryfy datanodes:["
                    + Arrays.stream(Optional.ofNullable(datanodes).orElseGet(() -> new DatanodeInfo[0]))
                    .map((datanode) -> "("
                            + datanode.toString() + ":"
                            + datanode.getNetworkLocation()
                            + ")"
                    )
                    .collect(Collectors.joining(","))
                    + "], "
                    + "require_replica:" + require_replica
            );
        }

        if (require_replica <= 0) {
            return PLACEMENT_OK;
        } else if (datanodes == null) {
            return new CrossAZBlockBlockPlacementStatus(() -> "no datanode for placement");
        }

        Set<DatanodeInfo> deduplicated = Arrays.stream(datanodes).collect(Collectors.toSet());
        int selected = deduplicated.size();
        // not enough replica
        if (selected < require_replica) {
            return new CrossAZBlockBlockPlacementStatus(() -> "not enough locations:" + selected
                    + " datanodes:[" + deduplicated.stream()
                    .map(DatanodeInfo::toString).collect(Collectors.joining(","))
                    + "]"
                    + " for replication:" + require_replica);
        }

        if (this.estimator.isOptimal(deduplicated.stream()
                        .collect(Collectors.toCollection(() -> new ConcurrentSkipListSet<>(NODE_COMPARATOR))),
                require_replica)) {
            return PLACEMENT_OK;
        }

        return new CrossAZBlockBlockPlacementStatus(() ->
                "not enough distinct parents,require : " + require_replica
                        + " provided datanodes:["
                        + deduplicated.stream()
                        .map((datanode) -> "("
                                + datanode.toString() + ","
                                + topology.getNode(datanode.getNetworkLocation())
                                + ")"
                        ).collect(Collectors.joining(","))
                        + "]"
        );
    }

    @Override
    public List<DatanodeStorageInfo> chooseReplicasToDelete(
            Collection<DatanodeStorageInfo> candidates,
            int expectedNumOfReplicas,
            List<StorageType> excessTypes,
            DatanodeDescriptor addedNode,
            DatanodeDescriptor delNodeHint) {
        // deduplicate
        NavigableSet<DatanodeStorageInfo> storages = newStorageSet();
        storages.addAll(candidates);

        // replica is not enough, surely no eviction
        int to_evict = storages.size() - expectedNumOfReplicas;
        if (to_evict <= 0) {
            return Collections.emptyList();
        }

        // collect datanodes
        Map<DatanodeDescriptor, NavigableSet<DatanodeStorageInfo>> cluster_storage = clusterStorages(storages);

        // is placement optimal?
        NavigableSet<Node> datanodes = cluster_storage.keySet().stream()
                .collect(Collectors.toCollection(CrossAZBlockPlacementPolicy::newNodeSet));
        if (!estimator.isOptimal(datanodes, expectedNumOfReplicas)) {
            // no even optimal placement
            // remove nothing
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
                        .thenComparing((node) -> node.getDatanodeDescriptor().compareTo(delNodeHint) == 0 ? 0 : 1)
                        // respect added hint
                        .thenComparing((node) -> node.getDatanodeDescriptor().compareTo(addedNode) == 0 ? 1 : 0)
                        // heavy used first, note the minus sign
                        .thenComparing((node) -> -node.getDfsUsed())
                )
                .limit(to_evict)
                .collect(Collectors.toList());
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

        return Optional.ofNullable(resolved);
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
                                        .orElseGet(() -> "root,1"),
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

                    // 3. calculate keep child
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
