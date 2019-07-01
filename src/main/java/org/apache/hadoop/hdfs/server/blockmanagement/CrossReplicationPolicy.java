package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.Node;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class CrossReplicationPolicy extends BlockPlacementPolicy {

    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas, Node writer, List<DatanodeStorageInfo> chosen, boolean returnChosenNodes, Set<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy) {
        return new DatanodeStorageInfo[0];
    }

    @Override
    public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs, int numOfReplicas) {
        return null;
    }

    @Override
    public List<DatanodeStorageInfo> chooseReplicasToDelete(Collection<DatanodeStorageInfo> candidates, int expectedNumOfReplicas, List<StorageType> excessTypes, DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {
        return null;
    }

    @Override
    protected void initialize(Configuration conf, FSClusterStats stats, org.apache.hadoop.net.NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {

    }
}
