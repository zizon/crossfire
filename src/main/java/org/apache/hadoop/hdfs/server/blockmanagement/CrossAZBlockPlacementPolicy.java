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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CrossAZBlockPlacementPolicy extends BlockPlacementPolicy {

    public static final Log LOGGER = LogFactory.getLog(CrossAZBlockPlacementPolicy.class);

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
            .<DatanodeStorageInfo, String>comparing((storage) -> storage.getDatanodeDescriptor().getDatanodeUuid())
            .thenComparing(DatanodeStorageInfo::getStorageID);

    protected Configuration configuration;
    protected FSClusterStats stats;
    protected NetworkTopology topology;
    protected Host2NodesMap mapping;

    @Override
    public DatanodeStorageInfo[] chooseTarget(
            String path,
            int requrest_replica,
            Node writer,
            List<DatanodeStorageInfo> chosen,
            boolean returnChosenNodes,
            Set<Node> excludes,
            long blocksize,
            BlockStoragePolicy storage_policy) {
        //TODO
        return new DatanodeStorageInfo[0];
    }

    @Override
    public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] datanodes, int require_replica) {
        NavigableSet<DatanodeInfo> deduplicated = streamToSet(Arrays.stream(datanodes), NODE_COMPARATOR);
        debugOn(() -> String.format(
                "verify for datanodes:%s, require_replica:%s",
                stringify(deduplicated, (datanode) -> String.format(
                        "(%s,%s,%s)",
                        datanode.toString(),
                        datanode.getNetworkLocation(),
                        datanode.getLevel()
                )),
                require_replica
        ));

        // do verify
        BlockPlacementStatus status = verifyBlockPlacement(deduplicated, require_replica);
        debugOn(() -> String.format(
                "verify ok:%b messsage:%s",
                status.isPlacementPolicySatisfied(),
                status.getErrorDescription()
        ));

        return status;
    }

    @Override
    public List<DatanodeStorageInfo> chooseReplicasToDelete(
            Collection<DatanodeStorageInfo> candidates,
            int expected_replica,
            List<StorageType> excess_types,
            DatanodeDescriptor adde_hint,
            DatanodeDescriptor delete_hint) {
        // filter failed storage
        NavigableSet<DatanodeStorageInfo> storages = streamToSet(
                candidates.stream()
                        .filter((storage) -> storage.getState() != DatanodeStorage.State.FAILED),
                STORAGE_COMPARATOR
        );
        debugOn(() -> String.format(
                "before remove:%s",
                storageStringify(storages)
        ));

        int max_eviction = storages.size() - expected_replica;
        if (max_eviction <= 0) {
            // not enough, no eviction
            return Collections.emptyList();
        }

        // priority eviction
        Comparator<DatanodeStorageInfo> evicition_priority = evictionPriority(
                excess_types,
                adde_hint,
                delete_hint
        );

        // calculate hierarchy
        NavigableSet<DatanodeStorageInfo> removals = new TreeSet<>(STORAGE_COMPARATOR);
        Map<Node, NavigableSet<DatanodeStorageInfo>> storages_under_node = storagesUnderNode(storages);
        Map<Integer, NavigableSet<Node>> nodes_at_level = levelToNodes(storages_under_node.keySet());

        nodes_at_level.entrySet().stream()
                .sorted((left, right) -> right.getKey() - left.getKey())
                .forEachOrdered((level_to_nodes) -> {
                    if (removals.size() >= max_eviction) {
                        // satisfied
                        return;
                    }

                    int level = level_to_nodes.getKey();
                    NavigableSet<Node> nodes = level_to_nodes.getValue();
                    debugOn(() -> String.format(
                            "for level:%d nodes:%s",
                            level,
                            stringify(nodes, Node::toString)
                    ));

                    nodes.stream()
                            // 1. get storages cluster of each node
                            .map((node) -> new AbstractMap.SimpleImmutableEntry<>(node, storages_under_node.get(node)))
                            // 2. filter already removed
                            .map((node_with_storage) -> {
                                Node node = node_with_storage.getKey();
                                NavigableSet<DatanodeStorageInfo> node_storages = node_with_storage.getValue();
                                node_storages.removeAll(removals);

                                debugOn(() -> String.format(
                                        "node:%s with storage:%s",
                                        node,
                                        storageStringify(node_storages)
                                ));

                                return node_storages;
                            })
                            // 3. sort by size,descending order
                            .sorted(Comparator.comparing((cluster) -> -cluster.size()))
                            .forEachOrdered((cluster) -> {
                                if (removals.size() >= max_eviction) {
                                    // satisfied
                                    return;
                                }

                                debugOn(() -> String.format(
                                        "for level:%d  before removal:%s remvoed:%s",
                                        level,
                                        storageStringify(cluster),
                                        storageStringify(removals)
                                ));

                                // 4. keep only one child of this level
                                cluster.stream()
                                        .max(evicition_priority)
                                        .ifPresent((keep) -> {
                                            // add all to removal except keep
                                            cluster.remove(keep);
                                            removals.addAll(cluster);

                                            debugOn(() -> String.format(
                                                    "for level:%d  keep:%s removing:%s",
                                                    level,
                                                    storageStringify(Collections.singleton(keep)),
                                                    storageStringify(cluster)
                                            ));

                                            // add back
                                            cluster.clear();
                                            cluster.add(keep);
                                        });

                                debugOn(() -> String.format(
                                        "for level:%s after remove:%s",
                                        level,
                                        storageStringify(cluster)
                                ));
                            });
                });


        List<DatanodeStorageInfo> removed = removals.stream()
                .limit(max_eviction)
                .collect(Collectors.toList());

        debugOn(() -> String.format(
                "after, remvoed:%s",
                storageStringify(removed)
        ));

        return removed;
    }

    protected BlockPlacementStatus verifyBlockPlacement(
            NavigableSet<? extends DatanodeInfo> datanodes,
            int require_replica) {
        if (require_replica <= 0) {
            return PLACEMENT_OK;
        } else if (datanodes == null) {
            return new CrossAZBlockBlockPlacementStatus(() -> "no datanode for placement");
        }


        int selected = datanodes.size();
        // not enough replica
        if (selected < require_replica) {
            return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                    "not enough locations, got:%s datanodes:[%s] for replication:%s",
                    selected,
                    datanodes.stream()
                            .map(DatanodeInfo::toString)
                            .collect(Collectors.joining(",")),
                    require_replica
            ));
        }

        // 1. calculate expected level
        int replica_level = Integer.highestOneBit(require_replica);

        // 2. figure out topologys
        Map<Integer, NavigableSet<Node>> full_nodes_under_level = levelToNodes(this.topology.getLeaves(NodeBase.ROOT));
        Map<Integer, NavigableSet<Node>> nodes_under_level = levelToNodes(new ArrayList<>(datanodes));

        // 3. adjust expected_level
        int expected_level = Math.min(
                replica_level,
                full_nodes_under_level.keySet().stream()
                        .max(Integer::compareTo)
                        .orElse(1)
        );

        // 4. check each level
        for (int level = 0; level <= expected_level; level++) {
            int current_level = level;
            int full_replica_for_level = full_nodes_under_level.get(current_level).size();
            int selection = nodes_under_level.get(current_level).size();

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
                        "require replica:%d, at level %d expect full replica:%d == selection:%d, but fail to meet it",
                        require_replica,
                        current_level,
                        full_replica_for_level,
                        selection
                ));
            }

            // or current level can hold all replica.
            // each node at this level should had at most one replica.
            Map<Node, NavigableSet<Node>> groupby_parent = clusterBy(
                    datanodes.stream(),
                    (node) -> resolveToLevel(node, current_level).orElse(null)
            );

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

    protected Comparator<DatanodeStorageInfo> evictionPriority(List<StorageType> excess_types,
                                                               DatanodeDescriptor adde_hint,
                                                               DatanodeDescriptor delete_hint) {

        return Comparator.<DatanodeStorageInfo>comparingInt(
                // fail node first
                (node) -> node.getState() == DatanodeStorage.State.FAILED ? 0 : 1
        ).thenComparing(
                //  exceed type first
                (node) -> Optional.ofNullable(excess_types)
                        .map((exceeds) -> exceeds.remove(node.getStorageType()))
                        .orElse(false)
                        ? 0 : 1
        ).thenComparing(
                // delete hint first
                (node) -> Optional.ofNullable(delete_hint)
                        .map((hint) -> hint.compareTo(node.getDatanodeDescriptor()))
                        .orElse(0) != 0
                        ? 0 : 1
        ).thenComparing(
                // added hint last
                (node) -> Optional.ofNullable(adde_hint)
                        .map((hint) -> hint.compareTo(node.getDatanodeDescriptor()))
                        .orElse(0) == 0
                        ? 0 : 1
        ).thenComparing(
                // less storage first
                DatanodeStorageInfo::getRemaining
        );
    }

    protected <T> NavigableSet<T> streamToSet(Stream<T> stream, Comparator<? super T> comparator) {
        return stream.collect(Collectors.toCollection(() -> new TreeSet<>(comparator)));
    }

    protected <KEY, VALUE extends Node> Map<KEY, NavigableSet<VALUE>> clusterBy(
            Stream<? extends VALUE> stream,
            Promise.PromiseFunction<VALUE, KEY> key_mapping) {
        return stream.collect(
                Collectors.groupingBy(
                        key_mapping,
                        Collectors.toCollection(() -> new TreeSet<>(NODE_COMPARATOR))
                )
        );
    }

    protected <T extends Node> Map<Integer, NavigableSet<Node>> levelToNodes(Collection<T> nodes) {
        return this.clusterBy(
                nodes.stream()
                        .flatMap((node) -> {
                            Iterator<Node> iterator = LazyIterators.generate(
                                    (Node) node,
                                    Optional::ofNullable,
                                    (context, new_value) -> context.getParent()
                            );
                            return LazyIterators.stream(iterator);
                        }),
                Node::getLevel
        );
    }

    protected Map<Node, NavigableSet<DatanodeStorageInfo>> storagesUnderNode(Collection<DatanodeStorageInfo> storages) {
        return storages.stream()
                .flatMap((storage) -> {
                    Iterator<Node> iterator = LazyIterators.generate(
                            (Node) storage.getDatanodeDescriptor(),
                            Optional::ofNullable,
                            (context, new_value) -> context.getParent()
                    );

                    return LazyIterators.stream(iterator)
                            .map((node) -> new AbstractMap.SimpleImmutableEntry<>(node, storage));
                })
                .collect(Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.mapping(
                                Map.Entry::getValue,
                                Collectors.toCollection(() -> new TreeSet<>(STORAGE_COMPARATOR))
                        )
                ));
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

    protected String storageStringify(Collection<DatanodeStorageInfo> storages) {
        return stringify(storages, (storage) -> String.format(
                "(%s:%s,%s,%s,,%s)",
                storage.getDatanodeDescriptor().getIpAddr(),
                storage.getDatanodeDescriptor().getIpcPort(),
                storage.getDatanodeDescriptor().getNetworkLocation(),
                storage.getDatanodeDescriptor().getLevel(),
                storage.getStorageID()
                )
        );
    }

    protected void debugOn(Promise.PromiseSupplier<String> message) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(message.get());
        }
    }

    protected <T> String stringify(Collection<T> collection, Promise.PromiseFunction<T, String> to_string) {
        return String.format(
                "[%s]",
                collection.stream()
                        .map(to_string)
                        .collect(Collectors.joining(","))
        );
    }

    @Override
    protected void initialize(Configuration configuration, FSClusterStats stats, NetworkTopology topology,
                              Host2NodesMap mapping) {
        this.configuration = configuration;
        this.stats = stats;
        this.topology = topology;
        this.mapping = mapping;
    }
}
