package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestCrossAZBlockPlacementPolicy {

    public static final Log LOGGER = LogFactory.getLog(TestCrossAZBlockPlacementPolicy.class);


    NavigableSet<DatanodeInfo> datanodes;
    CrossAZBlockPlacementPolicy policy;
    List<DatanodeStorageInfo> storages;
    NetworkTopology topology;

    @Before
    public void setup() {
        storages = IntStream.range(1, 255)
                .mapToObj((i) -> {
                    String datacenter = i % 2 == 0 ? "even" : "odd";
                    String rack = "rack_" + i % 10;
                    String location = "/" + datacenter + "/" + rack;
                    String ip = "10.202.77." + i;
                    String hostname = "datanode_" + i;
                    DatanodeID id = new DatanodeID(ip, hostname, UUID.randomUUID().toString(), 0, 0, 0, 0);
                    DatanodeDescriptor datanode = new DatanodeDescriptor(id, location);

                    datanode.updateHeartbeat(
                            Arrays.stream(DatanodeStorage.State.values())
                                    .flatMap((state) ->
                                            Arrays.stream(StorageType.values())
                                                    .map((type) -> {
                                                                DatanodeStorageInfo info = new DatanodeStorageInfo(
                                                                        datanode,
                                                                        new DatanodeStorage(
                                                                                DatanodeStorage.generateUuid(),
                                                                                state,
                                                                                type
                                                                        )
                                                                );

                                                                long capacity = ThreadLocalRandom.current().nextLong(1024L * 1024L * 1024L * 1024L * 2L);
                                                                long used = ThreadLocalRandom.current().nextLong(capacity);
                                                                info.setUtilizationForTesting(capacity, used, capacity - used, ThreadLocalRandom.current().nextLong(used));
                                                                return info.toStorageReport();
                                                            }
                                                    )
                                    ).toArray(StorageReport[]::new),
                            0,
                            0,
                            ThreadLocalRandom.current().nextInt(40),
                            0,
                            null
                    );

                    //DatanodeStorage storage = new DatanodeStorage();
                    return Arrays.stream(datanode.getStorageInfos());
                })
                .flatMap(Function.identity())
                .collect(Collectors.toList());
        datanodes = storages.stream()
                .map(DatanodeStorageInfo::getDatanodeDescriptor)
                .collect(Collectors.toCollection(() -> new TreeSet<>(CrossAZBlockPlacementPolicy.NODE_COMPARATOR)));

        policy = new CrossAZBlockPlacementPolicy();
        topology = new NetworkTopology();
        datanodes.forEach(topology::add);
        policy.initialize(null, null, topology, null);
    }

    protected DatanodeInfo[] selectSubset(String datacenter, String rack) {
        return datanodes.stream()
                .filter((datanode) -> datanode.getNetworkLocation().contains(datacenter))
                .filter((datanode) -> datanode.getNetworkLocation().contains(rack))
                .toArray(DatanodeInfo[]::new);
    }

    protected void helpTestVerifyBlockPlacement(int replica, boolean satisfied, String message, DatanodeInfo... datanodes) {
        LOGGER.info("test case ------------------------");
        BlockPlacementStatus status = policy.verifyBlockPlacement(datanodes, replica);
        if (satisfied) {
            Assert.assertTrue(message + status, status.isPlacementPolicySatisfied());
        } else {
            Assert.assertFalse(message + status, status.isPlacementPolicySatisfied());
        }
    }

    @Test
    public void testVerifyBlockPlacement() {
        DatanodeInfo[] even_rack_2 = selectSubset("even", "rack_2");
        DatanodeInfo[] even_rack_4 = selectSubset("even", "rack_4");
        DatanodeInfo[] even_rack_6 = selectSubset("even", "rack_6");
        DatanodeInfo[] odd_rack_1 = selectSubset("odd", "rack_1");
        DatanodeInfo[] odd_rack_3 = selectSubset("odd", "rack_3");

        //             root      --level 0
        //             / \
        //          odd          --level 1
        //          /
        //         1             --level 2
        //        /
        //      (0)              --level 3(leaf)
        helpTestVerifyBlockPlacement(3, false, "under replication", datanodes.first());

        // null test
        helpTestVerifyBlockPlacement(3, false, "null datanode");

        //             root      --level 0
        //             / \
        //          odd          --level 1
        //          /
        //         1             --level 2
        //       /
        //     (1)               --level 3(leaf)
        helpTestVerifyBlockPlacement(3, false, "same datanode test:",
                odd_rack_1[0], odd_rack_1[0], odd_rack_1[0]
        );

        //             root      --level 0
        //             / \
        //          even          --level 1
        //         /\ \
        //       2  4  6          --level 2
        //       |  |  |
        //      (1)(1)(1)         --level 3(leaf)
        helpTestVerifyBlockPlacement(3, false, "same datacenter:",
                even_rack_2[0], even_rack_4[0], even_rack_6[0]
        );

        //             root      --level 0
        //             /  \
        //          even  odd    --level 1
        //         /\    /
        //       2  4   1        --level 2
        //       |  |   |
        //      (1)(1) (1)       --level 3
        helpTestVerifyBlockPlacement(3, true, "multi datacenter:",
                even_rack_2[0], even_rack_4[0], odd_rack_1[0]
        );

        //             root      --level 0
        //             /  \
        //          even  odd    --level 1
        //         /\    /
        //       2  4   1        --level 2
        //       |  |   |
        //      (1)(1) (1)       --level 3(leaf)
        helpTestVerifyBlockPlacement(5, false, " over replica but under replication with multi datacenter:",
                even_rack_2[0], even_rack_4[0], odd_rack_1[0]
        );

        //             root      --level 0
        //             /   \
        //          even   odd    --level 1
        //         /\ \     / \
        //       2  4 6    1  3    --level 2
        //       |  | |    |  |
        //      (1)(1)(1) (1)(1)   --level 3(leaf)
        helpTestVerifyBlockPlacement(5, true, "fully replica distributed:",
                even_rack_2[0], even_rack_4[0], even_rack_6[0], odd_rack_1[0], odd_rack_3[0]
        );

        //             root      --level 0
        //             /   \
        //          even   odd    --level 1
        //         /\      / \
        //       2  4     1  3    --level 2
        //       |  |     |  |
        //      (1)(2)   (1)(1)   --level 3(leaf)
        helpTestVerifyBlockPlacement(5, false, "not balanced distributed:",
                even_rack_2[0], even_rack_4[0], even_rack_4[1], odd_rack_1[0], odd_rack_3[0]
        );

        //             root      --level 0
        //             /   \
        //          even         --level 1
        //         /\
        //       2  4           --level 2
        //       |  |
        //      (2)(3)          --level 3(leaf)
        helpTestVerifyBlockPlacement(5, false, "not balanced distributed:",
                even_rack_2[0], even_rack_2[1], even_rack_4[0], even_rack_4[1], even_rack_4[3]
        );
    }

    protected NavigableSet<DatanodeStorageInfo> buildSet(DatanodeStorage.State state, DatanodeInfo... datanodes) {
        return Arrays.stream(datanodes)
                .map((datanode) -> storages.stream()
                        .filter((storage) ->
                                storage.getDatanodeDescriptor().compareTo(datanode) == 0
                        )
                        .filter((storage) -> storage.getState() == state)
                        .findFirst()
                        .orElse(null)
                )
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(() -> new TreeSet<>(CrossAZBlockPlacementPolicy.STORAGE_COMPARATOR)));
    }

    protected void helpTestchooseReplicasToDelete(int replica, int failed_nodes, NavigableSet<DatanodeStorageInfo> storages, String messsage, boolean satisfied_after_remove) {
        List<DatanodeStorageInfo> selected = policy.chooseReplicasToDelete(storages, replica, Collections.emptyList(), null, null);
        int allow_to_remove = storages.size() - failed_nodes;
        int expected = allow_to_remove - replica;
        if (expected < 0) {
            expected = 0;
        }
        Assert.assertEquals(messsage, expected, selected.size());
        storages.removeAll(selected);
        BlockPlacementStatus status = policy.verifyBlockPlacement(
                storages.stream().map(DatanodeStorageInfo::getDatanodeDescriptor)
                        .toArray(DatanodeInfo[]::new),
                replica
        );
        Assert.assertEquals(String.format(
                "fail reason:%s removed:[%s] remainds:[%s]",
                status.getErrorDescription(),
                selected.stream().map((storage) -> String.format(
                        "(%s)",
                        storage
                )).collect(Collectors.joining(",")),
                storages.stream().map((storage) -> String.format(
                        "(%s)",
                        storage
                )).collect(Collectors.joining(","))
        ), status.isPlacementPolicySatisfied(), satisfied_after_remove);
    }

    @Test
    public void testchooseReplicasToDelete() {
        DatanodeInfo[] even_rack_2 = selectSubset("even", "rack_2");
        DatanodeInfo[] even_rack_4 = selectSubset("even", "rack_4");
        DatanodeInfo[] odd_rack_1 = selectSubset("odd", "rack_1");
        DatanodeInfo[] odd_rack_3 = selectSubset("odd", "rack_3");
        DatanodeInfo[] odd_rack_5 = selectSubset("odd", "rack_5");

        //             root      --level 0
        //             /
        //          even        --level 1
        //         /
        //       2              --level 2
        //     / \ \
        //   2  12  22          --level 3
        LOGGER.info("-------------------------");
        NavigableSet<DatanodeStorageInfo> storages = buildSet(DatanodeStorage.State.NORMAL, even_rack_2[0], even_rack_2[1], even_rack_2[2]);
        helpTestchooseReplicasToDelete(3, 0, storages, "unsatisfied removeal", false);

        //                  root    --levle 0
        //                  /
        //               even      --level 1
        //             /    \
        //            2      4     --level 2
        //         /\  \    / \  \
        //       2  12 22  4  14  24    --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(DatanodeStorage.State.FAILED, even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_4[0], even_rack_4[1], even_rack_4[2]);
        helpTestchooseReplicasToDelete(3, 6, storages, "unsatisfied removeal with exceeded and failed node", false);

        //                  root    --levle 0
        //                  /
        //               even      --level 1
        //             /    \
        //            2      4     --level 2
        //         /\  \    / \  \
        //       2  12 22  4  14  24    --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(DatanodeStorage.State.NORMAL, even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_4[0], even_rack_4[1], even_rack_4[2]);
        helpTestchooseReplicasToDelete(3, 0, storages, "unsatisfied removeal with exceeded node", false);

        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \  \   \
        //           2       4       1  3   5   --level 2
        //         /\  \    / \  \    \  \   \
        //       2  12 22  4  14  24   1  3  5  --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(DatanodeStorage.State.NORMAL, even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_4[0], even_rack_4[1], even_rack_4[2], odd_rack_1[0], odd_rack_3[0], odd_rack_5[0]);
        helpTestchooseReplicasToDelete(3, 0, storages, "satisfied with exceeded node case 1", true);

        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \
        //           2       4       1    --level 2
        //         /\  \    / \  \    \
        //       2  12 22  4  14  24   1  --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(DatanodeStorage.State.NORMAL, even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_4[0], even_rack_4[1], even_rack_4[2], odd_rack_1[0]);
        helpTestchooseReplicasToDelete(3, 0, storages, "satisfied with exceeded node case 2", true);

        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \
        //           2       4       1    --level 2
        //         /       / \  \    \
        //       2       4  14  24   1  --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(DatanodeStorage.State.NORMAL, even_rack_2[0], even_rack_4[0], even_rack_4[1], even_rack_4[2], odd_rack_1[0]);
        helpTestchooseReplicasToDelete(3, 0, storages, "satisfied with exceeded node case 2", true);

        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \
        //           2       4       1    --level 2
        //         /       / \  \    \
        //       2       4  14  24   1  --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(DatanodeStorage.State.NORMAL, even_rack_2[0], even_rack_4[0], even_rack_4[1], even_rack_4[2], odd_rack_1[0]);
        helpTestchooseReplicasToDelete(4, 0, storages, "satisfied with exceeded node case 3", true);

        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \
        //           2             1    --level 2
        //         /  \            |
        //       2    12           1  --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(DatanodeStorage.State.NORMAL, even_rack_2[0], even_rack_2[1], odd_rack_1[0]);
        helpTestchooseReplicasToDelete(3, 0, storages, "satisfied with exceeded node case 3", false);

        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \
        //           2      4       1    --level 2
        //         /       |       |
        //       2        4       1  --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(DatanodeStorage.State.NORMAL, even_rack_2[0], even_rack_4[0], odd_rack_1[0]);
        helpTestchooseReplicasToDelete(3, 0, storages, "satisfied with exceeded node case 3", true);
    }

    @Test
    public void testchooseTargetForNewBlock() {
        BlockStoragePolicy storage_policy = BlockStoragePolicySuite.createDefaultSuite().getDefaultPolicy();
        NavigableSet<DatanodeStorageInfo> storages = Arrays.stream(policy.chooseTarget(null, 3, null, null, 12, null, storage_policy))
                .collect(Collectors.toCollection(() -> new TreeSet<>(CrossAZBlockPlacementPolicy.STORAGE_COMPARATOR)));
        BlockPlacementStatus status = policy.verifyBlockPlacement(
                storages.stream()
                        .map(DatanodeStorageInfo::getDatanodeDescriptor)
                        .toArray(DatanodeInfo[]::new),
                3
        );
        Assert.assertTrue(status.getErrorDescription(), status.isPlacementPolicySatisfied());
    }

    @Test
    public void test() {
        StorageCluster cluster = new StorageCluster(topology, topology.getLeaves(NodeBase.ROOT));
        //LOGGER.info(cluster.root);
        cluster.children().stream()
                .map(cluster::find)
                //.map(Map.Entry::getKey)
                //.flatMap(Collection::stream)
                .forEach(LOGGER::info);
    }
}
