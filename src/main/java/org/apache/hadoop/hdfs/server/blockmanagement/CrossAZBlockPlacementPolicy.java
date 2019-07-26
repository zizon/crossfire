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

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
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
            int reqeusting,
            Node writer,
            List<DatanodeStorageInfo> chosen,
            boolean returnChosenNodes,
            Set<Node> excludes,
            long block_size,
            BlockStoragePolicy storage_policy) {
        //todo
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
        List<StorageType> storage_types = storage_policy.chooseStorageTypes((short) num_of_replicas);
        Map<Node, NavigableSet<DatanodeStorageInfo>> storages_under_node = selectiveStorages(
                block_size,
                Optional.ofNullable(excludes).orElseGet(Collections::emptySet),
                Optional.ofNullable(storage_types).orElseGet(Collections::emptyList)
        );

        // 1. decided datacenter
        Predicate<Node> datacenter_predicator = Optional.ofNullable(writer)
                // try from writer
                .flatMap((initiator) -> resolveToLevel(initiator, 1))
                .map((datacenter) -> {
                    debugOn(() -> String.format(
                            "selecte datacenter:%s from writer:%s",
                            datacenter,
                            writer
                    ));
                    return datacenter;
                })
                .map(Collections::singleton)
                // or try from favored
                .orElseGet(
                        () -> Optional.ofNullable(favored)
                                .<Set<Node>>map((prefers -> prefers.stream()
                                        .map((node) -> resolveToLevel(node, 1))
                                        .filter(Optional::isPresent)
                                        .map(Optional::get)
                                        .collect(Collectors.toCollection(() -> new TreeSet<>(NODE_COMPARATOR)))
                                ))
                                .orElseGet(Collections::emptySet)
                ).stream()
                .findFirst()
                .map((datacenter) -> {
                    debugOn(() -> String.format(
                            "select datacneter from favored node:%s datacenter:%s",
                            stringify(favored, (datanode) -> String.format(
                                    "%s,%s,%s",
                                    datanode.getNetworkLocation(),
                                    datanode.getLevel(),
                                    datanode.getDatanodeUuid()
                            )),
                            datacenter
                    ));
                    return datacenter;
                })
                .<Predicate<Node>>map((datacenter) ->
                        (node) -> {
                            debugOn(() -> String.format(
                                    "use datacenter favor:%s",
                                    datacenter
                            ));

                            return resolveToLevel(node, 1)
                                    .map((resovled) -> NODE_COMPARATOR.compare(resovled, node) == 0)
                                    .orElse(false);
                        }
                ).orElseGet(() -> {
                    debugOn(() -> "not using datacenter favor");
                    return (ignore) -> true;
                });

        // 2. stick to datacenter predicator
        storages_under_node.entrySet().stream()
                .map((entry) -> {
                    NavigableSet<DatanodeStorageInfo> cluster = entry.getValue();
                    // remove nodes not in datacenter
                    cluster.removeIf((storage) -> {
                        boolean same_datacenter = datacenter_predicator.test(storage.getDatanodeDescriptor());
                        debugOn(() -> String.format(
                                "remove storage:%s for located in different datacenter",
                                storageStringify(Collections.singleton(storage))
                        ));
                        return same_datacenter;
                    });
                    return cluster.isEmpty() ? entry.getKey() : null;
                })
                .filter(Objects::nonNull)
                // clear useless nodes
                .collect(Collectors.toCollection(() -> new TreeSet<>(NODE_COMPARATOR)))
                .forEach(storages_under_node::remove);

        // 3. flatten to level
        Map<Integer, NavigableSet<Node>> level_to_nodes = levelToNodes(storages_under_node.keySet());

        // 4. choose
        Comparator<DatanodeStorageInfo> favor_priority = evictionPriority(
                Collections.emptyList(),
                null,
                null
        );
        NavigableSet<DatanodeStorageInfo> chosen = new TreeSet<>(STORAGE_COMPARATOR);
        level_to_nodes.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .forEachOrdered((entry) -> {
                    if (chosen.size() >= num_of_replicas) {
                        // satisfied
                        return;
                    }

                    int level = entry.getKey();
                    NavigableSet<Node> nodes_at_level = entry.getValue();

                    NavigableSet<Node> chosen_nodes_at_level = chosen.stream()
                            .map((node) -> resolveToLevel(node.getDatanodeDescriptor(), level))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.toCollection(() -> new TreeSet<>(NODE_COMPARATOR)));

                    // select one for each node
                    entry.getValue().stream()
                            .forEach((node_at_level) -> {
                                if (chosen.size() >= num_of_replicas) {
                                    // satisfied
                                    return;
                                }

                                NavigableSet<DatanodeStorageInfo> candidates = storages_under_node.get(node_at_level);
                                candidates.stream()
                                        .max(favor_priority
                                                .thenComparing((storage) -> {
                                                    boolean node_already_chosen = resolveToLevel(storage.getDatanodeDescriptor(), level)
                                                            .map((resovled_node) -> NODE_COMPARATOR.compare(
                                                                    resovled_node,
                                                                    node_at_level
                                                            ) == 0)
                                                            .orElse(false);

                                                    // favor not selected nodes
                                                    return node_already_chosen ? -1 : 0;
                                                }))
                                        .ifPresent(chosen::add);

                                // remove chosen
                                candidates.removeAll(chosen);
                            });
                });


        // done
        return Stream.of(
                chosen.stream(),
                storages_under_node.values().stream()
                        .flatMap(Collection::stream)
                        .distinct()
                        // randome drop
                        .filter((ignore) -> ThreadLocalRandom.current().nextBoolean())
                        .sorted(favor_priority.reversed())
        ).flatMap(Function.identity())
                .limit(num_of_replicas)
                .toArray(DatanodeStorageInfo[]::new);
    }

    protected BlockPlacementStatus placementOptimal(StorageCluster.StorageNode node, Map<String, StorageCluster.StorageNode> provided, int require_replica) {
        debugOn(() -> String.format(
                "test for node:%s provided:%d/[%s] replica:%d",
                node,
                provided.size(),
                provided.values().stream()
                        .map((candidate) -> String.format(
                                "(%s)",
                                candidate
                        ))
                        .collect(Collectors.joining(",")),
                require_replica
        ));

        Map<String, StorageCluster.StorageNode> assigned = node.children();
        int expected_groups = Math.min(require_replica, provided.size());
        if (assigned.size() != expected_groups) {
            return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                    "for node:%s, avaliable group:%d, expected place:%d, but assigned:%d, replica requirement:%d",
                    node,
                    provided.size(),
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
        int max_replica_per_group = require_replica / expected_groups + 1;
        for (StorageCluster.StorageNode selected : node.children().values()) {
            StorageCluster.StorageNode hint = provided.get(selected.name());
            if (hint == null) {
                return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                        "selected:%s not found in provided set:{%s}",
                        selected,
                        provided.values().stream().map((hited_node) -> String.format(
                                "(%s)",
                                hited_node
                        )).collect(Collectors.joining(","))
                ));
            }

            Set<StorageCluster.StorageNode> leaves = selected.leaves();
            if (leaves.size() > max_replica_per_group) {
                return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                        "node:%s leaves:%s exceed max groups:%s",
                        selected,
                        leaves.size(),
                        max_replica_per_group
                ));
            }

            BlockPlacementStatus status = placementOptimal(selected, hint.children(), leaves.size());
            if (!status.isPlacementPolicySatisfied()) {
                return status;
            }
        }

        return PLACEMENT_OK;
    }

    @Override
    public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] datanodes, int require_replica) {
        NavigableSet<Node> storage_nodes = Arrays.stream(datanodes)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(() -> new TreeSet<>(NODE_COMPARATOR)));
        if (storage_nodes.size() < require_replica) {
            return new CrossAZBlockBlockPlacementStatus(() -> String.format(
                    "not enough storage nodes:[%s], require:%s",
                    storage_nodes.stream()
                            .map((storage) -> String.format(
                                    "(%s)",
                                    storage
                            )).collect(Collectors.joining(",")),
                    require_replica
            ));
        }

        StorageCluster full_cluster = new StorageCluster(topology, topology.getLeaves(NodeBase.ROOT));
        StorageCluster constructed = new StorageCluster(topology, storage_nodes);

        return placementOptimal(constructed.root(), full_cluster.children(), require_replica);
        /*
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
        */
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

        /**
         *  given a hierarchy of placement, strategy to remove storages are:
         *
         *  1. aways remvoe from deepest leaf node, since it impact rack diversity in least level(a
         *  higher level node removal cause less data spread scope， e.g remove a rack node drop the whole rack, while
         *  a machine drop only the machine itself)
         *
         *  2. keep exactly one node for each parent. more children means more potential over replica.
         *  and removing the last child of a node will cause hierarchy collapse.
         *
         *  3. for each level, remove child from the most heavy node first, since it with least hierarchy impact.
         *
         */

        // calculate hierarchy
        NavigableSet<DatanodeStorageInfo> removals = new TreeSet<>(STORAGE_COMPARATOR);
        Map<Node, NavigableSet<DatanodeStorageInfo>> storages_under_node = storagesUnderNode(storages);
        Map<Integer, NavigableSet<Node>> nodes_at_level = levelToNodes(storages_under_node.keySet());

        nodes_at_level.entrySet().stream()
                // for each level, sort ndoes in ascending order
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

                    // for each nodes at this level
                    nodes.stream()
                            // 1. get storages cluster of each node.
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

                    // a more optimal approach is to remove in round robbin
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

    protected Map<Node, NavigableSet<DatanodeStorageInfo>> selectiveStorages(
            long avaliable_space,
            Set<Node> excludes,
            List<StorageType> storage_types) {
        return storagesUnderNode(this.topology.getLeaves(NodeBase.ROOT).stream()
                .filter((node) ->
                        // node path not in excludes
                        !LazyIterators.stream(LazyIterators.generate(
                                (Node) node,
                                Optional::ofNullable,
                                (context, new_value) -> context.getParent()
                                )
                        ).anyMatch(excludes::contains)
                )
                .filter((node) -> node instanceof DatanodeDescriptor)
                .map(DatanodeDescriptor.class::cast)
                .flatMap((datanode) -> Arrays.stream(datanode.getStorageInfos()))
                //.filter((storage) -> storage.getState() != DatanodeStorage.State.FAILED)
                //.filter((storage) -> storage_types.isEmpty() || storage_types.contains(storage.getStorageType()))
                //.filter((storage) -> storage.getRemaining() >= avaliable_space)
                .peek((storage) -> debugOn(() -> String.format(
                        "collect storage:%s",
                        storageStringify(Collections.singleton(storage))
                )))
                .collect(Collectors.toCollection(() -> new TreeSet<>(STORAGE_COMPARATOR)))
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
