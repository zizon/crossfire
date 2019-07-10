package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Reconfigurable;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class CrossAZBlockPlacementPolicy extends BlockPlacementPolicy {

    protected Configuration configuration;
    protected FSClusterStats stats;
    protected NetworkTopology topology;
    protected Host2NodesMap mapping;

    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas, Node writer, List<DatanodeStorageInfo> chosen, boolean returnChosenNodes, Set<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy) {
        return new DatanodeStorageInfo[0];
    }

    @Override
    public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locations, int require_replica) {
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
