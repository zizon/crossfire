package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fs.misc.LazyIterators;
import com.fs.misc.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
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

    protected static Comparator<Node> NODE_COMPARATOR = Comparator
            .comparingInt(Node::getLevel)
            .thenComparing(Node::getName);

    protected Configuration configuration;
    protected FSClusterStats stats;
    protected NetworkTopology topology;
    protected Host2NodesMap mapping;

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
    public List<DatanodeStorageInfo> chooseReplicasToDelete(
            Collection<DatanodeStorageInfo> candidates,
            int expectedNumOfReplicas,
            List<StorageType> excessTypes,
            DatanodeDescriptor addedNode,
            DatanodeDescriptor delNodeHint) {
        // TODO
        return null;
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

        if (isOptimal(deduplicated.stream()
                        .collect(Collectors.toCollection(() -> new TreeSet<>(NODE_COMPARATOR))),
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

    protected boolean isOptimal(NavigableSet<Node> racks, int require_replica) {
        switch (require_replica) {
            case 0:
                // no rack needed
                return true;
            case 1:
                // not empty rack is sufficient
                return !racks.isEmpty();
        }

        ConcurrentMap<Integer, NavigableSet<Node>> leveled_nodes = racks.parallelStream()
                .flatMap((node) -> {
                    Iterator<Node> iterator = LazyIterators.generate(
                            node,
                            Optional::ofNullable,
                            (context, new_value) -> {
                                LOGGER.info("context:" + context + " value:" + new_value);
                                return context.getParent();
                            }
                    );
                    return LazyIterators.stream(iterator);
                })
                .filter((node) -> node.getLevel() != 0)
                .collect(Collectors.groupingByConcurrent(
                        Node::getLevel,
                        Collectors.toCollection(() -> new TreeSet<>(NODE_COMPARATOR)))
                );

        boolean not_satisfied = leveled_nodes.entrySet().parallelStream()
                .map((entry) -> {
                    NavigableSet<Node> same_level = entry.getValue();

                    // at least two nodes require for each level
                    if (same_level.size() < 2) {
                        return false;
                    }

                    // cluster by parent
                    ConcurrentMap<Node, List<Node>> cluster_by_parent = same_level.parallelStream()
                            .collect(Collectors.groupingByConcurrent(Node::getParent));

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
                    boolean overload = cluster_by_parent.values().parallelStream()
                            .map((children) -> children.size() <= load_limit)
                            .anyMatch((satisfied) -> !satisfied);

                    return !overload;
                }).anyMatch((satisfied) -> !satisfied);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("racks:["
                    + leveled_nodes.entrySet().stream()
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

    @Override
    protected void initialize(Configuration configuration, FSClusterStats stats, NetworkTopology topology,
                              Host2NodesMap mapping) {
        this.configuration = configuration;
        this.stats = stats;
        this.topology = topology;
        this.mapping = mapping;
    }
}
