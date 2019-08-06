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
            this.reason = () -> "placement optimal";
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
            int additional,
            Node writer,
            List<DatanodeStorageInfo> provided,
            boolean return_chosen,
            Set<Node> exclusion,
            long block_size,
            BlockStoragePolicy storage_policy) {
        List<DatanodeStorageInfo> chosen = Optional.ofNullable(provided).orElseGet(Collections::emptyList);
        Set<Node> excludes = Optional.ofNullable(exclusion).orElseGet(Collections::emptySet);

        // find top level racks
        Set<String> top_level_racks = Stream.of(
                // resolve chosen to top level
                chosen.stream()
                        .map(DatanodeStorageInfo::getDatanodeDescriptor)
                        .map(this::maybeToTopLevelRack),
                // resolve write to top level
                Stream.of(maybeToTopLevelRack(writer)),
                excludes.stream()
        ).flatMap(Function.identity())
                .map(NodeBase::getPath)
                .map(this::toTopRack)
                // with given rack
                .filter((rack) -> !rack.equals(NetworkTopology.DEFAULT_RACK))
                .collect(Collectors.toSet());

        // selection root
        Node selection_root = topology.getNode(top_level_racks.size() > 1
                ? NodeBase.ROOT
                : top_level_racks.isEmpty()
                ? NodeBase.ROOT
                : top_level_racks.iterator().next()
        );

        // preference
        Set<StorageType> prefer_type = new HashSet<>(storage_policy.chooseStorageTypes((short) additional));
        Comparator<DatanodeStorageInfo> prefer = Comparator.<DatanodeStorageInfo>comparingInt(
                // prefer suggested storage
                (storage) -> prefer_type.contains(storage.getStorageType()) ? 0 : 1
        )// more free space first, round to 100GB
                .thenComparingLong((storage) -> -storage.getRemaining() / 1024 * 1024 * 1024 * 100)
                // less workload first
                .thenComparingInt((storage) -> storage.getDatanodeDescriptor().getXceiverCount());

        NetworkTopology currently_had = new NetworkTopology();
        chosen.stream().map(DatanodeStorageInfo::getDatanodeDescriptor).forEach(currently_had::add);

        // filter for storage
        Set<String> exclude_expressions = excludes.stream()
                .map(NodeBase::getPath)
                .collect(Collectors.toSet());
        Predicate<String> node_exclude_fileter = (node) -> exclude_expressions.stream().anyMatch(node::startsWith);
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
            boolean selected = currently_had.getNode(node_path) != null;

            return !selected;
        };

        // select
        DatanodeStorageInfo[] selected = sealTogether(
                selectInNode(
                        selection_root,
                        additional,
                        storage_filter,
                        node_exclude_fileter,
                        prefer,
                        currently_had
                ).limit(additional),
                chosen.stream(),
                return_chosen
        );

        if (selected.length < additional) {
            // more needed
            LOGGER.warn(String.format(
                    "expect %d datanodes but allocate only:%d/[%s], path:%s writer:%s/%s provided:[%s] exclusion:[%s]",
                    additional,
                    selected.length,
                    Arrays.stream(selected)
                            .map((storage) -> String.format(
                                    "(%s|%s)",
                                    storage,
                                    storage.getDatanodeDescriptor().getNetworkLocation()
                            ))
                            .collect(Collectors.joining(",")),
                    path,
                    writer,
                    writer.getNetworkLocation(),
                    chosen.stream()
                            .map((storage) -> String.format(
                                    "(%s|%s)",
                                    storage,
                                    storage.getDatanodeDescriptor().getNetworkLocation()
                            ))
                            .collect(Collectors.joining(",")),
                    excludes.stream()
                            .map(NodeBase::getPath)
                            .collect(Collectors.joining(","))
            ));
        }

        debugOn(() -> String.format(
                "selected:%d/[%s], require:%d, excldue:[%s], provided:[%s]  prefer storage type:[%s], block size:%d",
                selected.length,
                Arrays.stream(selected)
                        .map((storage) -> String.format(
                                "(%s|%s)",
                                storage,
                                storage.getDatanodeDescriptor().getNetworkLocation()
                        ))
                        .collect(Collectors.joining(",")),
                additional,
                excludes.stream()
                        .map((storage) -> String.format(
                                "(%s|%s)",
                                storage,
                                storage.getNetworkLocation()
                        )).collect(Collectors.joining(",")),
                chosen.stream().map((storage) -> String.format(
                        "(%s|%s)",
                        storage,
                        storage.getDatanodeDescriptor().getNetworkLocation()
                )).collect(Collectors.joining(",")),
                prefer_type,
                block_size
        ));

        return selected;
    }

    @Override
    public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] datanodes, int require_replica) {
        Set<DatanodeInfo> provided = Arrays.stream(datanodes)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(
                        () -> new TreeSet<>(Comparator.comparing(DatanodeInfo::getDatanodeUuid))
                ));

        if (provided.size() < require_replica) {
            return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                    "not enough storage nodes:[%s], require:%s",
                    provided.stream()
                            .map((node) -> String.format(
                                    "(%s)",
                                    node
                            ))
                            .collect(Collectors.joining(",")),
                    require_replica
            ));
        }

        NetworkTopology constructed = new NetworkTopology();
        provided.forEach(constructed::add);

        // fast path: replica optimal?
        if (require_replica < topology.getNumOfRacks()) {
            // each rack should had one
            if (constructed.getNumOfRacks() < require_replica) {
                return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                        "placement is not optimal, reburied replica:%d , distinct rack:%d, but place in:%d",
                        require_replica,
                        topology.getNumOfRacks(),
                        constructed.getNumOfRacks()
                ));
            }
        }

        // fast path: datanode optimal?
        if (provided.size() < topology.getNumOfRacks()) {
            // datanode should be place in distinct rack
            if (constructed.getNumOfRacks() != provided.size()) {
                return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                        "datanodes:%d can be place in:%d but place:%d",
                        provided.size(),
                        topology.getNumOfRacks(),
                        constructed.getNumOfRacks()
                ));
            }
        }

        // slow path: load balanced test
        Set<String> visited = new HashSet<>();
        for (DatanodeInfo datanode : provided) {
            for (Node tracking = datanode; tracking != null; tracking = tracking.getParent()) {
                String location = tracking.getNetworkLocation();
                if (!visited.add(location)) {
                    continue;
                }

                List<Node> placed_group = constructed.getDatanodesInRack(location);
                List<Node> available_group = topology.getDatanodesInRack(location);
                int placed = constructed.countNumOfAvailableNodes(location, Collections.emptyList());

                if (placed < placed_group.size()) {
                    LOGGER.warn(String.format(
                            "expect placed:%d >= placed_group:%d but not",
                            placed,
                            placed_group.size()
                    ));
                    continue;
                }

                // expect more groups, available?
                if (placed_group.size() < available_group.size()
                        && placed > placed_group.size()) {
                    // more group available,not optimal
                    return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                            "location:%s to place:%d in available:%d but placed:%d",
                            location,
                            placed,
                            available_group.size(),
                            placed_group.size()
                    ));
                }

                // placed use all available group
                // each group load equal?
                int min_load = 0;
                int max_load = 0;
                for (Node group : placed_group) {
                    int leaves = constructed.countNumOfAvailableNodes(NodeBase.getPath(group), Collections.emptyList());
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
                            cluster_by_datanode.computeIfPresent(
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
                                                       Predicate<String> node_exclude_filter,
                                                       Comparator<DatanodeStorageInfo> prefer,
                                                       NetworkTopology currently_had) {
        final int allocation_limit = expected_selection;
        if (expected_selection <= 0) {
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
                    .peek((storage) -> currently_had.add(storage.getDatanodeDescriptor()))
                    ;
        }

        // check group available
        String path = NodeBase.getPath(node);
        Set<String> available_group = Optional.ofNullable(topology.getDatanodesInRack(path))
                .orElseGet(Collections::emptyList).stream()
                .map(NodeBase::getPath)
                .filter((group) -> !node_exclude_filter.test(group))
                .collect(Collectors.toSet());
        if (available_group.size() == 0) {
            return Stream.empty();
        }

        Map<String, Integer> current_load = Optional.ofNullable(currently_had.getDatanodesInRack(path))
                .orElseGet(Collections::emptyList).stream()
                .collect(Collectors.toMap(
                        NodeBase::getPath,
                        (maybe_rack) -> currently_had.countNumOfAvailableNodes(
                                NodeBase.getPath(maybe_rack),
                                Collections.emptyList()
                        )
                ));

        // figure out allocation for each group
        Map<String, Integer> group_allocation = new HashMap<>();
        Set<String> new_group = available_group.stream().filter((group) -> !current_load.containsKey(group))
                .collect(Collectors.toSet());
        if (!new_group.isEmpty()) {
            if (expected_selection < new_group.size()) {
                // can be doen totally in new group
                return new_group.stream()
                        .map((group) -> new AbstractMap.SimpleImmutableEntry<>(group, ThreadLocalRandom.current().nextInt()))
                        .sorted(Comparator.comparingInt(Map.Entry::getValue))
                        .map((group) -> selectInNode(
                                topology.getNode(group.getKey()),
                                1,
                                storage_filter,
                                node_exclude_filter,
                                prefer,
                                currently_had
                        ))
                        .flatMap(Function.identity())
                        .limit(expected_selection)
                        ;
            }

            // fill new groups with at most the same load of currently had
            // allocation for each group
            int allocation = expected_selection / new_group.size();
            int max_load = current_load.values().stream()
                    .max(Integer::compareTo)
                    .orElse(0);

            // guard allocation under max load except it is zero
            int load = max_load > 0 ? Math.min(allocation, max_load) : allocation;
            new_group.forEach((group) -> group_allocation.put(group, load));

            // calculate tailing
            expected_selection = expected_selection - load * new_group.size();
        }

        // available groups are all touched
        Map<String, Integer> speculate_load = Stream.concat(
                current_load.entrySet().stream(),
                group_allocation.entrySet().stream()
        ).filter((group) -> !node_exclude_filter.test(group.getKey()))
                .collect(Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.collectingAndThen(
                                Collectors.summarizingInt(Map.Entry::getValue),
                                (statistics) -> (int) statistics.getSum()
                        )
                ));

        // assign
        IntStream.range(0, expected_selection)
                .forEach((ignore) -> {
                    // allocate at least load group
                    speculate_load.entrySet().stream()
                            .min(Comparator.comparingInt(Map.Entry::getValue))
                            .ifPresent((choice) -> {
                                // allocate
                                group_allocation.compute(
                                        choice.getKey(),
                                        (key, value) -> Optional.ofNullable(value)
                                                .orElse(0)
                                                + 1
                                );

                                // update load map
                                speculate_load.computeIfPresent(choice.getKey(), (key, value) -> value + 1);
                            });
                });

        // drill down
        return group_allocation.entrySet().stream()
                .map((allocation) -> selectInNode(
                        topology.getNode(allocation.getKey()),
                        allocation.getValue(),
                        storage_filter,
                        node_exclude_filter,
                        prefer,
                        currently_had
                ))
                .flatMap(Function.identity())
                .limit(allocation_limit);
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

    protected DatanodeStorageInfo[] sealTogether(Stream<DatanodeStorageInfo> must, Stream<DatanodeStorageInfo> maybe, boolean include_maybe) {
        if (!include_maybe) {
            return must.toArray(DatanodeStorageInfo[]::new);
        }

        return Stream.concat(
                must,
                maybe
        ).toArray(DatanodeStorageInfo[]::new);
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
