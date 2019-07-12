package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Reconfigurable;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import javax.rmi.CORBA.Tie;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CrossAZBlockPlacementPolicy extends BlockPlacementPolicy {

    protected Configuration configuration;
    protected FSClusterStats stats;
    protected NetworkTopology topology;
    protected Host2NodesMap mapping;

    protected static class Tier {

        protected ConcurrentMap<String, Tier> children;
        protected String name;

        public Tier(String name) {
            this.name = name;
            this.children = new ConcurrentHashMap<>();
        }

        public void add(String[] levels) {
            Tier current = this;
            for (String sub : levels) {
                current = current.children.compute(sub, (key, child) -> {
                    if (child == null) {
                        child = new Tier(key);
                    }

                    return child;
                });
            }
        }

        public boolean satisfied(int require_replica) {
            int active_zone = children.size();
            if (active_zone >= require_replica) {
                return true;
            }

            if (children.size() == 0) {
                // leaf node
            }

            int extract_replica = require_replica - active_zone;

            return children.values()
                    .parallelStream()
                    .anyMatch((child) -> child.satisfied(require_replica));
        }


    }

    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas, Node writer, List<DatanodeStorageInfo> chosen, boolean returnChosenNodes, Set<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy) {
        return new DatanodeStorageInfo[0];
    }

    @Override
    public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locations, int require_replica) {
        Tier root = new Tier("root");
        for (DatanodeInfo datanode : locations) {
            root.add(datanode.getNetworkLocation().split("-"));
        }


        //TODO
        return null;
    }

    @Override
    public List<DatanodeStorageInfo> chooseReplicasToDelete(Collection<DatanodeStorageInfo> candidates, int expectedNumOfReplicas, List<StorageType> excessTypes, DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {
        return null;
    }

    @Override
    protected void initialize(Configuration configuration, FSClusterStats stats, NetworkTopology topology, Host2NodesMap mapping) {
        this.configuration = configuration;
        this.stats = stats;
        this.topology = topology;
        this.mapping = mapping;
    }
}
