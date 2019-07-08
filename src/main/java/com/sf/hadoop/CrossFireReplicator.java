package com.sf.hadoop;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CrossFireReplicator {

    protected DNSToSwitchMapping resolver;
    protected BlockPlacementPolicy placement_policy;

    protected int require_replication;

    public void scheudleReplication(LocatedBlock block) {
        List<DatanodeInfo> current = Arrays.stream(block.getLocations()).collect(Collectors.toList());
        List<String> location = resolver.resolve(current.parallelStream().map(DatanodeInfo::getIpAddr).collect(Collectors.toList()));


    }


}
