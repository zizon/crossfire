package org.apache.hadoop.hdfs.server.blockmanagement;

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

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
            int reqeusting,
            Node writer,
            List<DatanodeStorageInfo> chosen,
            boolean returnChosenNodes,
            Set<Node> excludes,
            long block_size,
            BlockStoragePolicy storage_policy) {
        //TODO
        return this.chooseTarget(
                path,
                reqeusting,
                writer,
                Optional.ofNullable(excludes)
                        .orElseGet(Collections::emptySet),
                block_size,
                Collections.emptyList(),
                storage_policy
        );
    }

    @Override
    DatanodeStorageInfo[] chooseTarget(String path,
                                       int num_of_replicas,
                                       Node writer,
                                       Set<Node> excludes,
                                       long block_size,
                                       List<DatanodeDescriptor> favored,
                                       BlockStoragePolicy storage_policy) {
        //TODO
        return null;
    }

    protected BlockPlacementStatus placementOptimal(StorageCluster.StorageNode node, StorageCluster cluster, int require_replica) {
        StorageCluster.StorageNode provided = cluster.find(node.index());
        debugOn(() -> String.format(
                "test for node:(%s) provided:%d/[%s] replica:%d",
                node,
                provided.children().size(),
                provided.children().stream()
                        .map((candidate) -> String.format(
                                "(%s)",
                                candidate
                        ))
                        .collect(Collectors.joining(",")),
                require_replica
        ));

        Set<String> assigned = node.children();
        int expected_groups = Math.min(require_replica, provided.children().size());
        if (assigned.size() != expected_groups) {
            return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                    "for node:%s, avaliable group:%d, expected place:%d, but assigned:%d, replica requirement:%d",
                    node,
                    provided.children().size(),
                    expected_groups,
                    assigned.size(),
                    require_replica
            ));
        }

        // no group required
        if (expected_groups == 0) {
            return PLACEMENT_OK;
        }

        // node has expected placement group.
        // calculate group load
        int max_replica_per_group = require_replica % expected_groups == 0
                ? require_replica / expected_groups
                : require_replica / expected_groups + 1;
        for (String selected_index : node.children()) {
            StorageCluster.StorageNode selected = node.cluster().find(selected_index);

            Set<String> leaves = selected.leaves().collect(Collectors.toSet());
            if (leaves.size() > max_replica_per_group) {
                return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                        "node:%s leaves:%s exceed max groups:%s",
                        selected,
                        leaves.size(),
                        max_replica_per_group
                ));
            }

            BlockPlacementStatus status = placementOptimal(selected, cluster, leaves.size());
            if (!status.isPlacementPolicySatisfied()) {
                return status;
            }
        }

        return PLACEMENT_OK;
    }

    @Override
    public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] datanodes, int require_replica) {
        if (datanodes.length < require_replica) {
            return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                    "not enough storage nodes:[%s], require:%s",
                    Arrays.stream(datanodes)
                            .map((node) -> String.format(
                                    "(%s)",
                                    node
                            ))
                            .collect(Collectors.joining(",")),
                    require_replica
            ));
        }

        NetworkTopology constructed = new NetworkTopology();
        Arrays.stream(datanodes).forEach(constructed::add);

        // fast path: replica optimal?
        if (require_replica < topology.getNumOfRacks()) {
            // each rack should had one
            if (constructed.getNumOfRacks() != require_replica) {
                return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                        "placement is not optimal, requrie replica:%d < distinct rack:%d but place in:%d",
                        require_replica,
                        topology.getNumOfRacks(),
                        constructed.getNumOfRacks()
                ));
            }
        }

        // fast path: datanode optimal?
        if (datanodes.length < topology.getNumOfRacks()) {
            // datanode should be place in distinct rack
            if (constructed.getNumOfRacks() != datanodes.length) {
                return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                        "datanodes:%d can be place in:%d but place:%d",
                        datanodes.length,
                        topology.getNumOfRacks(),
                        constructed.getNumOfRacks()
                ));
            }
        }

        // slow path: load balanced test
        Set<String> checked = new HashSet<>();
        for (DatanodeInfo datanode : datanodes) {
            for (Node tracking = datanode; tracking != null; tracking = tracking.getParent()) {
                String location = tracking.getNetworkLocation();
                if (!checked.add(location)) {
                    continue;
                }

                List<Node> placed_group = constructed.getDatanodesInRack(location);
                List<Node> available_group = topology.getDatanodesInRack(location);
                List<Node> placed = constructed.getLeaves(location);

                if (placed.size() < placed_group.size()) {
                    LOGGER.warn(String.format(
                            "expect placed:%d >= placed_group:%d but not",
                            placed.size(),
                            placed_group.size()
                    ));
                    continue;
                }

                // expect more groups, available?
                if (placed_group.size() < available_group.size()
                        && placed.size() > placed_group.size()) {
                    // more group available,not optimal
                    return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                            "location:%s to place:%d in available:%d but placed:%d",
                            location,
                            placed.size(),
                            available_group.size(),
                            placed_group.size()
                    ));
                }

                // placed use all available group
                // each group load equal?
                int min_load = 0;
                int max_load = 0;
                for (Node group : placed_group) {
                    int leaves = constructed.getLeaves(NodeBase.getPath(group)).size();
                    min_load = Math.min(min_load == 0 ? leaves : min_load, leaves);
                    max_load = Math.max(leaves, max_load);
                }

                if (max_load - min_load > 1) {
                    int final_max_load = max_load;
                    int final_min_load = min_load;
                    return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                            "location:%s load not balanced, min:%d max:%d of group:[%s]",
                            location,
                            final_min_load,
                            final_max_load,
                            placed_group.stream()
                                    .map((node) -> String.format(
                                            "(%s:[%s])",
                                            node,
                                            constructed.getLeaves(NodeBase.getPath(node)).stream()
                                                    .map((leaf) -> String.format(
                                                            "%s",
                                                            leaf
                                                    )).collect(Collectors.joining(":,"))
                                    )).collect(Collectors.joining(","))
                    ));
                }
            }
        }

        return PLACEMENT_OK;
    }

    protected StorageCluster cluster() {
        return new StorageCluster(topology, topology.getLeaves(NodeBase.ROOT));
    }

    protected List<DatanodeStorageInfo> tryRemoveAtNode(
            StorageCluster.StorageNode node,
            Map<String, Set<DatanodeStorageInfo>> cluster_by_node,
            Comparator<DatanodeStorageInfo> evicition_priority) {
        List<DatanodeStorageInfo> remvoe_candiates = node.leaves()
                .map(cluster_by_node::get)
                .flatMap(Collection::stream)
                .sorted(evicition_priority)
                .collect(Collectors.toList());

        if (remvoe_candiates.size() <= 1) {
            return Collections.emptyList();
        }

        return remvoe_candiates.subList(1, remvoe_candiates.size());
    }

    @Override
    public List<DatanodeStorageInfo> chooseReplicasToDelete(
            Collection<DatanodeStorageInfo> candidates,
            int expected_replica,
            List<StorageType> excess_types,
            DatanodeDescriptor adde_hint,
            DatanodeDescriptor delete_hint) {
        StorageCluster constructed = new StorageCluster(
                topology,
                candidates.stream()
                        .filter((storage) -> storage.getState() != DatanodeStorage.State.FAILED)
                        .map(DatanodeStorageInfo::getDatanodeDescriptor)
                        .collect(Collectors.toList())
        );

        Map<String, Set<DatanodeStorageInfo>> cluster_by_node = candidates.stream()
                .collect(Collectors.groupingBy(
                        (storage) -> StorageCluster.index(storage.getDatanodeDescriptor()),
                        Collectors.toCollection(() -> new TreeSet<>(STORAGE_COMPARATOR))
                ));

        // calculate active nodes
        int active = (int) cluster_by_node.values().stream()
                .collect(Collectors.summarizingInt(Collection::size))
                .getSum();

        if (active <= expected_replica) {
            return Collections.emptyList();
        }

        int max_removal = active - expected_replica;
        Map<String, DatanodeStorageInfo> to_remove = new HashMap<>();
        // priority eviction
        Comparator<DatanodeStorageInfo> evicition_priority = evictionPriority(
                excess_types,
                adde_hint,
                delete_hint
        );

        Queue<String> processing_queue = new LinkedList<>(cluster_by_node.keySet());
        while (!processing_queue.isEmpty()) {
            if (to_remove.size() >= max_removal) {
                break;
            }

            StorageCluster.StorageNode not_fail = constructed.find(processing_queue.poll());
            if (not_fail == null) {
                continue;
            }

            tryRemoveAtNode(
                    not_fail,
                    cluster_by_node,
                    evicition_priority
            ).forEach((storage) -> {
                if (to_remove.size() >= max_removal) {
                    return;
                }

                // remove from cluster_by_node
                cluster_by_node.computeIfPresent(
                        StorageCluster.index(storage.getDatanodeDescriptor()),
                        (key, storages_under_node) -> {
                            storages_under_node.remove(storage);
                            return storages_under_node;
                        }
                );

                // add to removed
                to_remove.put(storage.getStorageID(), storage);
            });
            processing_queue.offer(not_fail.parentIndex());
        }

        // ensure live nodes
        boolean any_live = cluster_by_node.values().stream()
                .flatMap(Collection::stream)
                .anyMatch((storage) -> storage.getState() != DatanodeStorage.State.FAILED);

        if (any_live) {
            return new ArrayList<>(to_remove.values());
        }

        return Collections.emptyList();
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

    protected void debugOn(Promise.PromiseSupplier<String> message) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(message.get());
        }
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
