package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestCrossAZBlockPlacementPolicy {

    public static final Log LOGGER = LogFactory.getLog(TestCrossAZBlockPlacementPolicy.class);


    protected List<DatanodeInfo> datanodes;

    @Before
    public void setup() {
        datanodes = IntStream.range(1, 255)
                .mapToObj((i) -> {
                    String datacenter = i % 2 == 0 ? "even" : "odd";
                    String rack = "rack_" + i % 10;
                    String location = "/" + datacenter + "/" + rack;
                    String ip = "10.202.77." + i;
                    String hostname = "datanode_" + i;
                    DatanodeID id = new DatanodeID(ip, hostname, UUID.randomUUID().toString(), 0, 0, 0, 0);
                    return new DatanodeInfo(id, location);
                }).collect(Collectors.toList());
    }

    protected DatanodeInfo[] selectSubset(String datacenter, String rack) {
        return datanodes.stream()
                .filter((datanode) -> datanode.getNetworkLocation().contains(datacenter))
                .filter((datanode) -> datanode.getNetworkLocation().contains(rack))
                .toArray(DatanodeInfo[]::new);
    }

    protected BlockPlacementStatus verify(BlockPlacementPolicy policy, int replica, DatanodeInfo... datanodes) {
        return policy.verifyBlockPlacement(datanodes, replica);
    }

    @Test
    public void testVerifyBlockPlacement() {
        BlockPlacementPolicy policy = new CrossAZBlockPlacementPolicy();
        NetworkTopology topology = new NetworkTopology();
        datanodes.forEach(topology::add);

        policy.initialize(null, null, topology, null);

        DatanodeInfo[] even = selectSubset("even", "");
        DatanodeInfo[] odd = selectSubset("odd", "");
        DatanodeInfo[] even_rack_2 = selectSubset("even", "rack_2");
        DatanodeInfo[] even_rack_4 = selectSubset("even", "rack_4");
        DatanodeInfo[] odd_rack_1 = selectSubset("odd", "rack_1");
        DatanodeInfo[] odd_rack_3 = selectSubset("odd", "rack_3");

        //             root      --level 0
        //             / \
        //          odd
        //          /
        //         1
        BlockPlacementStatus status = verify(policy, 3, datanodes.get(0));
        Assert.assertFalse("under replication" + status, status.isPlacementPolicySatisfied());

        status = verify(policy, 3);
        Assert.assertFalse("null datanode" + status, status.isPlacementPolicySatisfied());

        //             root      --level 0
        //             / \
        //          odd
        //           /
        //          1
        status = verify(policy, 3, datanodes.get(0), datanodes.get(0), datanodes.get(0));
        Assert.assertFalse("same datanode test:" + status, status.isPlacementPolicySatisfied());

        //             root      --level 0
        //             /
        //          even      --level 1
        //         /\ \
        //       2  4  6          --level 2
        status = verify(policy, 3, even[0], even[1], even[2]);
        Assert.assertFalse("same datacenter:" + status, status.isPlacementPolicySatisfied());

        //             root      --level 0
        //             /  \
        //          even  odd    --level 1
        //         /\    /
        //       2  6   3       --level 2
        status = verify(policy, 3, even[0], odd[1], even[2]);
        Assert.assertTrue("multi datacenter:" + status, status.isPlacementPolicySatisfied());

        //             root      --level 0
        //             /  \
        //          even  odd    --level 1
        //         /\    /
        //       2  6   3       --level 2
        status = verify(policy, 5, even[0], odd[1], even[2]);
        Assert.assertFalse(" over replica but under replication with multi datacenter:" + status,
                status.isPlacementPolicySatisfied());

        //             root      --level 0
        //             /   \
        //          even   odd    --level 1
        //         /\ \     / \
        //       2  4 8    5  7    --level 2
        status = verify(policy, 5, even_rack_2[0], even_rack_2[1], odd_rack_1[2], odd_rack_1[3], even_rack_2[4]);
        Assert.assertTrue("fully replica distributed:" + status,
                status.isPlacementPolicySatisfied());

        //               root      --level 0
        //             /     \
        //          even     odd    --level 1
        //        /\ \  \    / \
        //       2 12 22 32 1  11    --level 2
        status = verify(policy, 5, odd_rack_1[0], odd_rack_1[1], even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_2[3]);
        Assert.assertFalse("not balanced distributed:" + status,
                status.isPlacementPolicySatisfied());
    }
}
