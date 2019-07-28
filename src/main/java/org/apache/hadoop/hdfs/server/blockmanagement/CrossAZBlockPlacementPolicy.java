package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fs.misc.Promise;
import com.google.common.base.Strings;
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
                    Node current = tracking;
                    return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                            "location:%s to place:%d in available:%d but placed:%d",
                            NodeBase.getPath(current),
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
        if (candidates.size() <= expected_replica) {
            return Collections.emptyList();
        }

        NetworkTopology constructed = new NetworkTopology();
        Comparator<DatanodeStorageInfo> comparator = (left, right) -> {
            DatanodeDescriptor left_node = left.getDatanodeDescriptor();
            DatanodeDescriptor right_node = right.getDatanodeDescriptor();

            int compared = left_node.getDatanodeUuid().compareTo(right_node.getDatanodeUuid());

            // same datanode
            if (compared == 0) {
                // fail node first
                if (left.getState() == DatanodeStorage.State.FAILED
                        && right.getState() != DatanodeStorage.State.FAILED) {
                    return -1;
                } else if (right.getState() == DatanodeStorage.State.FAILED) {
                    return 1;
                }

                // both not fail,
                // less usable first
                return Long.compareUnsigned(left.getRemaining(), right.getRemaining());
            }

            // same rack?
            String left_location = left_node.getNetworkLocation();
            String right_location = right_node.getNetworkLocation();
            compared = left_location.compareTo(right_location);
            if (compared == 0) {
                // less space first
                return -Long.compare(left.getRemaining(), right.getRemaining());
            }

            // different rack
            // deeper first
            compared = -Integer.compare(left_node.getLevel(), right_node.getLevel());
            if (compared != 0) {
                return compared;
            }

            // differnt rack in same level
            // node the parent that has more children first
            for (; ; ) {
                List<Node> left_siblings = constructed.getDatanodesInRack(left_location);
                List<Node> righ_siblings = constructed.getDatanodesInRack(right_location);
                compared = -Integer.compare(left_siblings.size(), righ_siblings.size());
                if (compared != 0) {
                    return compared;
                }

                String left_parent_location = constructed.getNode(left_location).getNetworkLocation();
                String right_parent_location = constructed.getNode(right_location).getNetworkLocation();

                compared = left_parent_location.compareTo(right_parent_location);
                if (compared == 0) {
                    // reach same parent, compare by uuid
                    return left_location.compareTo(right_location);
                }

                left_location = left_parent_location;
                right_location = right_parent_location;
            }
        };

        // sort nodes
        List<DatanodeStorageInfo> storges = candidates.stream()
                .peek((storage) -> constructed.add(storage.getDatanodeDescriptor()))
                .sorted(comparator)
                .collect(Collectors.toList());
        List<DatanodeStorageInfo> log_reference = storges;
        debugOn(() -> String.format(
                "initial delete order:[%s]",
                log_reference.stream().map((node) -> String.format(
                        "(%s:%s)",
                        node,
                        node.getState()
                )).collect(Collectors.joining(","))
        ));

        int max_eviction = candidates.size() - expected_replica;
        List<DatanodeStorageInfo> to_remove = new ArrayList<>(max_eviction);
        while (to_remove.size() < max_eviction) {
            if (storges.isEmpty()) {
                // all removed,something wrong,give up
                LOGGER.warn("all removed,something wrong,give up");
                return Collections.emptyList();
            }

            DatanodeStorageInfo candidate = storges.get(0);
            to_remove.add(candidate);

            if (storges.size() > 1) {
                DatanodeStorageInfo next = storges.get(1);
                if (candidate.getDatanodeDescriptor().getName().compareTo(
                        next.getDatanodeDescriptor().getName()) != 0) {
                    // datanode consumed,remove from topology
                    constructed.remove(candidate.getDatanodeDescriptor());
                }
            }
            // resort
            List<DatanodeStorageInfo> new_storages = storges.subList(1, storges.size());
            new_storages.sort(comparator);

            debugOn(() -> String.format(
                    "select node:(%s) to delete, after delete order:[%s]",
                    candidate,
                    new_storages.stream().map((node) -> String.format(
                            "(%s:%s)",
                            node,
                            node.getState()
                    )).collect(Collectors.joining(","))
            ));

            storges = new_storages;
        }

        if (storges.stream().anyMatch((storage) -> storage.getState() != DatanodeStorage.State.FAILED)) {
            return to_remove;
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
