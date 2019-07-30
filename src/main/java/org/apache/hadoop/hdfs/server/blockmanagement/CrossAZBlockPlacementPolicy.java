package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fs.misc.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

    protected Configuration configuration;
    protected FSClusterStats stats;
    protected NetworkTopology topology;
    protected Host2NodesMap mapping;

    @Override
    public DatanodeStorageInfo[] chooseTarget(
            String path,
            int requesting,
            Node writer,
            List<DatanodeStorageInfo> chosen,
            boolean return_chosen,
            Set<Node> excludes,
            long block_size,
            BlockStoragePolicy storage_policy) {
        Set<String> top_levels = Stream.of(
                // writer top level reack
                Stream.of(maybeToTopLevelRack(writer)),
                Optional.ofNullable(chosen)
                        .orElseGet(Collections::emptyList).stream()
                        .map(DatanodeStorageInfo::getDatanodeDescriptor)
                        .map(this::maybeToTopLevelRack)
        ).flatMap(Function.identity())
                .map(NodeBase::getPath)
                .collect(Collectors.toSet());

        // already in different top level rack
        if (top_levels.size() > 1) {
            DatanodeStorageInfo[] selected = chooseTarget(
                    path,
                    requesting,
                    writer,
                    chosen.stream()
                            .map(DatanodeStorageInfo::getDatanodeDescriptor)
                            .collect(Collectors.toCollection(
                                    () -> new TreeSet<>(Comparator.comparing(NodeBase::getPath)))
                            ),
                    block_size,
                    Collections.emptyList(),
                    storage_policy
            );

            if (!return_chosen) {
                return selected;
            }

            return Stream.of(
                    Arrays.stream(selected),
                    chosen.stream()
            ).flatMap(Function.identity())
                    .toArray(DatanodeStorageInfo[]::new);
        }

        // located in one top rack
        // if it is from a  replication due to violate of placement
        // or from client pipeline patching?
        //TODO
        
        return this.chooseTarget(
                path,
                requesting,
                writer,
                Optional.ofNullable(excludes)
                        .orElseGet(Collections::emptySet),
                block_size,
                Collections.emptyList(),
                storage_policy
        );
    }


    /**
     * call when allocate a totaly new block
     *
     * @param path            the block file path
     * @param num_of_replicas require replica
     * @param writer          initial writer
     * @param excludes        exclude nodes
     * @param block_size      block size
     * @param favored         favored nodes
     * @param storage_policy  suggested storage policy
     * @return selected storage
     */
    @Override
    DatanodeStorageInfo[] chooseTarget(String path,
                                       int num_of_replicas,
                                       Node writer,
                                       Set<Node> excludes,
                                       long block_size,
                                       List<DatanodeDescriptor> favored,
                                       BlockStoragePolicy storage_policy) {
        Node rack_for_selection = maybeToTopLevelRack(writer);

        // preference
        Set<StorageType> prefer_type = new HashSet<>(storage_policy.chooseStorageTypes((short) num_of_replicas));
        Set<String> prefer_node = Optional.ofNullable(favored)
                .orElseGet(Collections::emptyList)
                .stream()
                .map(DatanodeID::getDatanodeUuid)
                .collect(Collectors.toSet());
        Comparator<DatanodeStorageInfo> prefer = Comparator.<DatanodeStorageInfo>comparingInt(
                // prefer nodes first
                (storage) -> prefer_node.contains(storage.getDatanodeDescriptor().getDatanodeUuid()) ? 0 : 1
        ).thenComparingInt(
                // prefer suggested storage
                (storage) -> prefer_type.contains(storage.getStorageType()) ? 0 : 1
        )// less workload first
                .thenComparingInt((storage) -> storage.getDatanodeDescriptor().getXceiverCount())
                // more free space first
                .thenComparingLong((storage) -> -storage.getRemaining());

        // exclusion
        Set<String> excluded_location = Optional.ofNullable(excludes).orElseGet(Collections::emptySet)
                .stream()
                .map(NodeBase::getPath)
                .collect(Collectors.toSet());

        // filter for storage
        Predicate<DatanodeStorageInfo> storage_filter = (storage) -> {
            // health storage
            if (storage.getState() == DatanodeStorage.State.FAILED) {
                return false;
            }

            // space ok
            if (storage.getRemaining() < block_size) {
                return false;
            }

            // right type
            if (!prefer_type.isEmpty() && !prefer_type.contains(storage.getStorageType())) {
                return false;
            }

            String node_path = NodeBase.getPath(storage.getDatanodeDescriptor());
            boolean is_exclude = excluded_location.stream().anyMatch(node_path::startsWith);

            return !is_exclude;
        };

        // select
        DatanodeStorageInfo[] selected = selectInNode(rack_for_selection, num_of_replicas, storage_filter, prefer)
                // for safety reason
                .limit(num_of_replicas)
                .toArray(DatanodeStorageInfo[]::new);

        debugOn(() -> String.format(
                "selected:%d/[%s], require:%d, excldue:[%s], favor:[%s], prefer storage type:[%s], block size:%d",
                selected.length,
                Arrays.stream(selected)
                        .map((storage) -> String.format(
                                "(%s|%s)",
                                storage,
                                storage.getDatanodeDescriptor().getNetworkLocation()
                        ))
                        .collect(Collectors.joining(",")),
                num_of_replicas,
                excluded_location,
                prefer_node,
                prefer_type,
                block_size
        ));

        return selected;
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
                        "placement is not optimal, requrie replica:%d , distinct rack:%d, but place in:%d",
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
        Set<String> visited = new HashSet<>();
        for (DatanodeInfo datanode : datanodes) {
            for (Node tracking = datanode; tracking != null; tracking = tracking.getParent()) {
                String location = tracking.getNetworkLocation();
                if (!visited.add(location)) {
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
        Comparator<DatanodeStorageInfo> comparator = selectForDeletion(constructed);

        // group storage by datanode,
        // it may not necessary but for defensive
        Map<String, NavigableSet<DatanodeStorageInfo>> cluster_by_datanode = candidates.stream()
                .filter(Objects::nonNull)
                .peek((storage) -> constructed.add(storage.getDatanodeDescriptor()))
                .collect(Collectors.groupingBy(
                        (storage) -> storage.getDatanodeDescriptor().getDatanodeUuid(),
                        Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(DatanodeStorageInfo::getStorageID)))
                ));

        // select removal
        List<DatanodeStorageInfo> to_remove = IntStream.range(0, candidates.size() - expected_replica)
                .mapToObj((ignore) -> cluster_by_datanode.values().stream()
                        .flatMap(Collection::stream)
                        .min(comparator)
                        .map((storage) -> {
                            // update topology due to storage removal
                            // since it matters ordering
                            cluster_by_datanode.compute(
                                    storage.getDatanodeDescriptor().getDatanodeUuid(),
                                    (key, storage_under_node) -> {
                                        // evict from set
                                        storage_under_node.remove(storage);

                                        // remove completely if empty
                                        if (storage_under_node.isEmpty()) {
                                            constructed.remove(storage.getDatanodeDescriptor());
                                            return null;
                                        }

                                        return storage_under_node;
                                    }
                            );

                            return storage;
                        })
                )
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (cluster_by_datanode.values().stream()
                .flatMap(Collection::stream)
                .anyMatch((storage) -> storage.getState() != DatanodeStorage.State.FAILED)) {
            return to_remove;
        }

        return Collections.emptyList();
    }

    protected Comparator<DatanodeStorageInfo> selectForDeletion(NetworkTopology constructed) {
        return (left, right) -> {
            DatanodeDescriptor left_node = left.getDatanodeDescriptor();
            DatanodeDescriptor right_node = right.getDatanodeDescriptor();

            // same datanode
            if (left_node.getDatanodeUuid().equals(right_node.getDatanodeUuid())) {
                // fail node first
                if (left.getState() == DatanodeStorage.State.FAILED
                        && right.getState() != DatanodeStorage.State.FAILED) {
                    return -1;
                } else if (right.getState() == DatanodeStorage.State.FAILED) {
                    return 1;
                }

                // both not fail,
                // less usable first
                return Long.compare(left.getRemaining(), right.getRemaining());
            }

            // same rack?
            String left_location = left_node.getNetworkLocation();
            String right_location = right_node.getNetworkLocation();
            if (left_location.equals(right_location)) {
                // less space first
                return -Long.compare(left.getRemaining(), right.getRemaining());
            }

            // different rack
            // deeper first
            int compared = -Integer.compare(left_node.getLevel(), right_node.getLevel());
            if (compared != 0) {
                return compared;
            }

            // different rack in same level
            Node left_tracking = left_node;
            Node right_tracking = right_node;
            for (; ; ) {
                List<Node> left_siblings = constructed.getDatanodesInRack(left_tracking.getNetworkLocation());
                List<Node> right_siblings = constructed.getDatanodesInRack(right_tracking.getNetworkLocation());

                // node with parent that has more child first
                compared = -Integer.compare(left_siblings.size(), right_siblings.size());
                if (compared != 0) {
                    return compared;
                }

                // parent has same number of children
                String left_parent = NodeBase.getPath(left_tracking.getParent());
                String right_parent = NodeBase.getPath(right_tracking.getParent());

                // same parent, compare by location
                if (left_parent.equals(right_parent)) {
                    return left_location.compareTo(right_location);
                }

                // rollup
                left_tracking = left_tracking.getParent();
                right_tracking = right_tracking.getParent();
            }
        };
    }

    protected Stream<DatanodeStorageInfo> selectInNode(Node node,
                                                       int expected_selection,
                                                       Predicate<DatanodeStorageInfo> storage_filter,
                                                       Comparator<DatanodeStorageInfo> prefer) {
        if (expected_selection == 0) {
            return Stream.empty();
        }

        if (node instanceof DatanodeDescriptor) {
            return Arrays.stream(((DatanodeDescriptor) node).getStorageInfos())
                    // health storage
                    .filter(storage_filter)
                    // max available
                    .max(prefer)
                    .map(Stream::of)
                    .orElseGet(Stream::empty)
                    ;
        }

        String path = NodeBase.getPath(node);
        List<Node> available_group = topology.getDatanodesInRack(path);
        if (available_group.size() > expected_selection) {
            // place in distinct group
            return available_group.stream()
                    .map((group) -> selectInNode(group, 1, storage_filter, prefer))
                    .flatMap(Function.identity())
                    .sorted(prefer)
                    .limit(expected_selection);
        }

        // not enough group
        if (available_group.size() == 0) {
            return Stream.empty();
        }

        // shuffle
        Collections.shuffle(available_group, ThreadLocalRandom.current());

        // for each group
        // 1 = 11 / 5
        int load = expected_selection / available_group.size();
        return IntStream.range(0, available_group.size())
                .mapToObj((group) -> {
                    if (group + 1 != available_group.size()) {
                        return selectInNode(available_group.get(group), load, storage_filter, prefer);
                    }

                    // last group
                    return selectInNode(
                            available_group.get(group),
                            load + expected_selection % available_group.size(),
                            storage_filter,
                            prefer
                    );
                })
                .flatMap(Function.identity())
                .limit(expected_selection);
    }

    protected String toTopRack(String path) {
        int second_part = path.indexOf(NodeBase.PATH_SEPARATOR, 1);
        if (second_part != -1)
            return path.substring(0, second_part);
        return path;
    }

    protected Node maybeToTopLevelRack(Node writer) {
        String writer_toplevel_rack = Optional.ofNullable(writer)
                .map(NodeBase::getPath)
                .map(this::toTopRack)
                .orElseGet(() -> this.toTopRack(NetworkTopology.DEFAULT_RACK));

        Node rack_for_selection = topology.getNode(writer_toplevel_rack);
        if (rack_for_selection == null) {
            rack_for_selection = topology.getNode(NodeBase.ROOT);
        }

        return rack_for_selection;
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
