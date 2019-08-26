package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fs.misc.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CrossAZBlockPlacementPolicy extends BlockPlacementPolicy {

    public static final Log LOGGER = LogFactory.getLog(CrossAZBlockPlacementPolicy.class);

    public static String USER_FAST_VERIFY = "com.sf.crossaz.fast-verify";
    public static String DO_PLACEMENT_ONLY = "com.sf.crossaz.do-placement-only";

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
    protected boolean use_fast_verify;
    protected long stale_interval;
    protected boolean do_placement_only;

    public void updateFastVerify(boolean enable) {
        this.use_fast_verify = enable;
        LOGGER.info(String.format(
                "update fast verify to:%b",
                enable
        ));
        this.configuration.setBoolean(USER_FAST_VERIFY, enable);
    }

    public void updateStaleInterval(long interval) {
        LOGGER.info(String.format(
                "update stale inerval to:%d",
                interval
        ));
        this.stale_interval = interval;
    }

    public boolean isFastVerifyEnable() {
        return this.use_fast_verify;
    }

    public void updateDoPlacementOnly(boolean do_placement_only) {
        this.do_placement_only = do_placement_only;
    }

    public boolean isDoPlacementOnly() {
        return this.do_placement_only;
    }

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
        EnumMap<StorageType, Long> prefer_type = storage_policy.chooseStorageTypes((short) additional).stream()
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        () -> new EnumMap<>(StorageType.class),
                        Collectors.collectingAndThen(
                                Collectors.summarizingInt((value) -> 1),
                                IntSummaryStatistics::getSum
                        )
                ));
        Comparator<DatanodeStorageInfo> prefer = Comparator.<DatanodeStorageInfo>comparingInt(
                // prefer suggested storage
                (storage) -> prefer_type.containsKey(storage.getStorageType()) ? 0 : 1
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
        Predicate<String> node_exclude_filter = (node) -> exclude_expressions.stream().anyMatch(node::startsWith);
        Predicate<DatanodeStorageInfo> storage_filter = (storage) -> {
            // health storage
            switch (storage.getState()) {
                case FAILED:
                case READ_ONLY_SHARED:
                    return false;
            }

            // space ok
            if (storage.getRemaining() < block_size) {
                return false;
            }

            // right type
            if (!prefer_type.isEmpty() && !prefer_type.containsKey(storage.getStorageType())) {
                return false;
            }

            String node_path = NodeBase.getPath(storage.getDatanodeDescriptor());
            boolean selected = currently_had.getNode(node_path) != null;

            return !selected;
        };
        Predicate<DatanodeDescriptor> datanode_filter = healthNodeTester();
        Consumer<DatanodeStorageInfo> on_consume = (storage) -> {
            // mark selected
            currently_had.add(storage.getDatanodeDescriptor());

            // update storage preference
            prefer_type.computeIfPresent(storage.getStorageType(), (type, value) -> {
                value = value - 1;
                if (value < 0) {
                    return null;
                }

                return value;
            });
        };

        // select
        DatanodeStorageInfo[] selected = sealTogether(
                selectInNode(
                        selection_root,
                        additional,
                        storage_filter,
                        node_exclude_filter,
                        prefer,
                        currently_had,
                        datanode_filter,
                        on_consume
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
                    Optional.ofNullable(writer)
                            .map(Node::getNetworkLocation)
                            .orElse("unknown"),
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
        if (do_placement_only) {
            return PLACEMENT_OK;
        }

        if (use_fast_verify) {
            return verifyBlockPlacementFast(datanodes, require_replica);
        }

        return verifyBlockPlacementBalancedOptimal(datanodes, require_replica);
    }

    @Override
    public List<DatanodeStorageInfo> chooseReplicasToDelete(
            Collection<DatanodeStorageInfo> candidates,
            int config_replica,
            List<StorageType> excess_types,
            DatanodeDescriptor adde_hint,
            DatanodeDescriptor delete_hint) {
        int expected_replica = config_replica;
        if (topology.getDatanodesInRack(NodeBase.ROOT).size() == 2 && config_replica > 1) {
            // special case for 2 datacenter, with require replication
            // keep 2 of each at least
            expected_replica = Math.min(4, config_replica);
        }

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

    protected Predicate<DatanodeDescriptor> healthNodeTester() {
        return (node) -> node.isRegistered()
                && !node.isDecommissionInProgress()
                && !node.isDecommissioned()
                && !node.isDisallowed()
                && !node.isStale(this.stale_interval);
    }

    protected BlockPlacementStatus verifyBlockPlacementFast(DatanodeInfo[] datanodes, int require_replica) {
        if (datanodes.length >= require_replica) {
            return PLACEMENT_OK;
        }

        return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                "expect replica:%s, but got:%s",
                require_replica,
                datanodes.length
        ));
    }

    protected BlockPlacementStatus verifyBlockPlacementBalancedOptimal(DatanodeInfo[] datanodes, int require_replica) {
        Map<String, LongSummaryStatistics> hierarchy = Arrays.stream(datanodes)
                .filter(Objects::nonNull)
                .flatMap((datanode) -> {
                    // build parent child pair
                    Stream.Builder<Map.Entry<String, String>> parent_child = Stream.builder();
                    for (Node current = datanode; current.getParent() != null; current = current.getParent()) {
                        parent_child.accept(new AbstractMap.SimpleImmutableEntry<>(
                                NodeBase.getPath(current.getParent()),
                                NodeBase.getPath(current)
                        ));
                    }

                    return parent_child.build();
                })
                .collect(
                        Collectors.groupingBy(
                                // greoup by parent
                                Map.Entry::getKey,
                                Collectors.collectingAndThen(
                                        // group by children statistics
                                        // then merge into one
                                        Collectors.groupingBy(
                                                Map.Entry::getValue,
                                                Collectors.summarizingInt((value) -> 1)
                                        ),
                                        (children) -> children.values().stream()
                                                .collect(Collectors.summarizingLong(IntSummaryStatistics::getSum))
                                )
                        )
                );

        boolean replica_num_ok = Optional.ofNullable(
                hierarchy.get(
                        NodeBase.getPath(topology.getNode(NodeBase.ROOT)))
        ).map((root) -> root.getSum() >= require_replica)
                .orElse(false);
        if (!replica_num_ok) {
            return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                    "not enough storage nodes:[%s], require:%s",
                    Arrays.stream(datanodes)
                            .filter(Objects::nonNull)
                            .map((node) -> String.format(
                                    "(%s)",
                                    node
                            ))
                            .collect(Collectors.joining(",")),
                    require_replica
            ));
        }

        boolean not_optimal = hierarchy.entrySet().stream()
                // find any not optimal
                .anyMatch((entry) -> {
                    String parent = entry.getKey();

                    // calcualte totoal placement of this parent
                    LongSummaryStatistics children_statistics = entry.getValue();

                    long leaf = children_statistics.getSum();
                    long placed_group = children_statistics.getCount();
                    long max_placed = children_statistics.getMax();
                    long min_placed = children_statistics.getMin();

                    // 1. find topology child of parent parent
                    long available = topology.getDatanodesInRack(parent).size();

                    // 2. can be place in different group?
                    if (leaf <= available) {
                        if (placed_group != leaf) {
                            debugOn(() -> String.format(
                                    "rack:%s not optimal,leaf:%d can be place in distinct group:%d , but place in:%d",
                                    parent,
                                    leaf,
                                    available,
                                    placed_group
                            ));

                            // not optimal
                            return true;
                        }
                    } else if (placed_group != available) {
                        // not enough distinct group
                        debugOn(() -> String.format(
                                "rack:%s not optimal,leaf:%s shoud be place in all available greoup:%d, but place:%d",
                                parent,
                                leaf,
                                available,
                                placed_group
                        ));

                        // not optimal
                        return true;
                    }

                    // if balanced?
                    boolean not_balanced = max_placed - min_placed > 1;

                    debugOn(() -> String.format(
                            "rack:%s balanced:%b place in group:%d, max:%d min:%d average:%f available:%d leaf:%d",
                            parent,
                            !not_balanced,
                            placed_group,
                            max_placed,
                            min_placed,
                            children_statistics.getAverage(),
                            available,
                            leaf
                    ));

                    // note: balance is optimal
                    return not_balanced;
                });

        if (not_optimal) {
            Promise.PromiseSupplier<String> reason = () -> String.format(
                    "placement not optimal, datanodes:[%s], require replica:%d",
                    Arrays.stream(datanodes)
                            .map((node) -> String.format(
                                    "(%s)",
                                    node
                            ))
                            .collect(Collectors.joining(",")),
                    require_replica
            );
            debugOn(reason);

            return new CrossAZBlockBlockPlacementStatus(reason);
        }

        return PLACEMENT_OK;
    }

    protected Comparator<DatanodeStorageInfo> selectForDeletion(NetworkTopology constructed) {
        Comparator<DatanodeStorageInfo> health_first = Comparator.comparingInt(
                (storage) -> storage.getState() == DatanodeStorage.State.FAILED ? -1 : 0
        );

        // for same datanode
        // fail stoarge first,
        // then less space first
        Comparator<DatanodeStorageInfo> same_datanode = health_first
                .thenComparingLong(DatanodeStorageInfo::getRemaining);

        // for same rack
        // less space first
        Comparator<DatanodeStorageInfo> same_rack = health_first
                .thenComparingLong(DatanodeStorageInfo::getRemaining);

        return (left, right) -> {
            DatanodeDescriptor left_node = left.getDatanodeDescriptor();
            DatanodeDescriptor right_node = right.getDatanodeDescriptor();

            // same datanode
            if (left_node.getDatanodeUuid().equals(right_node.getDatanodeUuid())) {
                return same_datanode.compare(left, right);
            }

            // same rack?
            String left_location = left_node.getNetworkLocation();
            String right_location = right_node.getNetworkLocation();
            if (left_location.equals(right_location)) {
                // less space first
                return same_rack.compare(left, right);
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
                    return same_rack.compare(left, right);
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
                                                       NetworkTopology currently_had,
                                                       Predicate<DatanodeDescriptor> datanode_filter,
                                                       Consumer<DatanodeStorageInfo> on_consume) {
        final int allocation_limit = expected_selection;
        if (expected_selection <= 0) {
            return Stream.empty();
        }

        if (node instanceof DatanodeDescriptor) {
            return Stream.of((DatanodeDescriptor) node)
                    .filter(datanode_filter)
                    .flatMap((datanode) -> Arrays.stream(datanode.getStorageInfos()))
                    .filter(storage_filter)
                    // max available
                    .max(prefer)
                    .map(Stream::of)
                    .orElseGet(Stream::empty)
                    .peek(on_consume)
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
                                currently_had,
                                datanode_filter,
                                on_consume
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
                        currently_had,
                        datanode_filter,
                        on_consume
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
        this.use_fast_verify = Optional.ofNullable(this.configuration.get(USER_FAST_VERIFY))
                .map(Boolean::parseBoolean)
                .orElse(true);
        this.stale_interval = configuration.getLong(
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
        this.do_placement_only = Optional.ofNullable(this.configuration.get(DO_PLACEMENT_ONLY))
                .map(Boolean::parseBoolean)
                .orElse(true);
    }
}
