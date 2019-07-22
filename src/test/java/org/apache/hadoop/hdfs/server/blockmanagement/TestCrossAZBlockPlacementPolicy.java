package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.SortedSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestCrossAZBlockPlacementPolicy {

    public static final Log LOGGER = LogFactory.getLog(TestCrossAZBlockPlacementPolicy.class);


    NavigableSet<DatanodeInfo> datanodes;
    CrossAZBlockPlacementPolicy policy;
    List<DatanodeStorageInfo> storages;

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

                    Stream.Builder<DatanodeStorageInfo> builder = Stream.builder();
                    for (DatanodeStorage.State state : DatanodeStorage.State.values()) {
                        for (StorageType type : StorageType.values()) {
                            DatanodeStorage storage = new DatanodeStorage(DatanodeStorage.generateUuid(), state, type);
                            builder.accept(new DatanodeStorageInfo(datanode, storage));
                        }
                    }

                    //DatanodeStorage storage = new DatanodeStorage();
                    return builder.build();
                })
                .flatMap(Function.identity())
                .collect(Collectors.toList());
        datanodes = storages.stream()
                .map(DatanodeStorageInfo::getDatanodeDescriptor)
                .collect(Collectors.toCollection(CrossAZBlockPlacementPolicy::newNodeSet));

        policy = new CrossAZBlockPlacementPolicy();
        NetworkTopology topology = new NetworkTopology();
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
        BlockPlacementStatus status = policy.verifyBlockPlacement(datanodes, replica);
        if (satisfied) {
            Assert.assertTrue(message + status, status.isPlacementPolicySatisfied());
        } else {
            Assert.assertFalse(message + status, status.isPlacementPolicySatisfied());
        }
    }

    @Test
    public void testVerifyBlockPlacement() {
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
        helpTestVerifyBlockPlacement(3, false, "under replication", datanodes.first());

        // null test
        helpTestVerifyBlockPlacement(3, false, "null datanode");

        //             root      --level 0
        //             / \
        //          odd
        //           /
        //          1
        helpTestVerifyBlockPlacement(3, false, "same datanode test:",
                datanodes.first(), datanodes.first(), datanodes.first()
        );

        //             root      --level 0
        //             /
        //          even      --level 1
        //         /\ \
        //       2  4  6          --level 2
        helpTestVerifyBlockPlacement(3, false, "same datacenter:",
                even[0], even[1], even[2]
        );

        //             root      --level 0
        //             /  \
        //          even  odd    --level 1
        //         /\    /
        //       2  6   3       --level 2
        helpTestVerifyBlockPlacement(3, true, "multi datacenter:",
                even[0], odd[1], even[2]
        );

        //             root      --level 0
        //             /  \
        //          even  odd    --level 1
        //         /\    /
        //       2  6   3       --level 2
        helpTestVerifyBlockPlacement(5, false, " over replica but under replication with multi datacenter:",
                even[0], odd[1], even[2]
        );

        //             root      --level 0
        //             /   \
        //          even   odd    --level 1
        //         /\ \     / \
        //       2  4 8    5  7    --level 2
        helpTestVerifyBlockPlacement(5, true, "fully replica distributed:",
                even_rack_2[0], even_rack_2[1], odd_rack_1[2], odd_rack_1[3], even_rack_2[4]
        );

        //               root      --level 0
        //             /     \
        //          even     odd    --level 1
        //        /\ \  \    / \
        //       2 12 22 32 1  11    --level 2
        helpTestVerifyBlockPlacement(5, false, "not balanced distributed:",
                odd_rack_1[0], odd_rack_1[1], even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_2[3]
        );
    }

    protected void printStorageInfo(Collection<DatanodeStorageInfo> storages) {
        LOGGER.info(String.format(
                "storages size:%d content:[%s]",
                storages.size(),
                storages.stream().map(DatanodeStorageInfo::toString)
        ));


    }

    protected List<DatanodeStorageInfo> helpTestEviction(NavigableSet<DatanodeStorageInfo> storages, int expected_keep, String message) {
        /*
        LOGGER.info("-----------------------------" + message);
        List<DatanodeStorageInfo> evicted = policy.evictExceededStoragePerDatanode(storages);
        printStorageInfo(storages);
        printStorageInfo(evicted);
        Assert.assertEquals(message, expected_keep, (storages.size() - evicted.size()));
        return evicted;
        */
        return null;
    }

    @Test
    public void testEvictExceededStoragePerDatanode() {
        Map<DatanodeDescriptor, NavigableSet<DatanodeStorageInfo>> cluster_by_datanode = storages.stream()
                .collect(Collectors.groupingBy(DatanodeStorageInfo::getDatanodeDescriptor,
                        Collectors.toCollection(CrossAZBlockPlacementPolicy::newStorageSet)));

        NavigableSet<DatanodeStorageInfo> storages = cluster_by_datanode.values().stream()
                .findAny()
                .orElseThrow(() -> new NullPointerException("no staorge found"));
        helpTestEviction(storages, 1, "storages in single node");

        storages = cluster_by_datanode.values().stream()
                .limit(2)
                .flatMap(NavigableSet::stream)
                .collect(Collectors.toCollection(CrossAZBlockPlacementPolicy::newStorageSet));
        helpTestEviction(storages, 2, "storage spread 2 nodes");

        storages = cluster_by_datanode.values().stream()
                .limit(10)
                .map(SortedSet::first)
                .collect(Collectors.toCollection(CrossAZBlockPlacementPolicy::newStorageSet));
        helpTestEviction(storages, 10, "spread across datanodes");
    }

    protected NavigableSet<DatanodeStorageInfo> buildSet(DatanodeInfo... datanodes) {
        return Arrays.stream(datanodes)
                .map((datanode) -> storages.stream()
                        .filter((storage) ->
                                storage.getDatanodeDescriptor().compareTo(datanode) == 0
                        )
                        .findFirst()
                        .orElse(null)
                )
                .filter(Objects::nonNull)
                .collect(CrossAZBlockPlacementPolicy.storageSetCollector());
    }


    @Test
    public void testPruneAtLevel() {
        DatanodeInfo[] even = selectSubset("even", "");
        DatanodeInfo[] odd = selectSubset("odd", "");
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
        NavigableSet<DatanodeStorageInfo> storages = buildSet(even_rack_2[0], even_rack_2[1], even_rack_2[2]);
        NavigableSet<DatanodeStorageInfo> selected = policy.pruneAtLevel(storages, 3);
        Assert.assertEquals("prune on for level 3", 1, selected.size());

        // remove none
        LOGGER.info("-------------------------");
        selected = policy.pruneAtLevel(storages, 2);
        Assert.assertEquals("empty for under optimal", 1, selected.size());

        //                  root    --levle 0
        //                  /
        //               even      --level 1
        //             /    \
        //            2      4     --level 2
        //         /\  \    / \  \
        //       2  12 22  4  14  24    --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_4[0], even_rack_4[1], even_rack_4[2]);
        selected = policy.pruneAtLevel(storages, 3);
        Assert.assertEquals("prune for level 3 with exceed balance node", 2, selected.size());

        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \
        //           2       4       1        --level 2
        //         /\  \    / \  \    \
        //       2  12 22  4  14  24   1     --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_4[0], even_rack_4[1], even_rack_4[2], odd_rack_1[0]);
        selected = policy.pruneAtLevel(storages, 2);
        Assert.assertEquals("prune for level 2 with under balanced nodes", 2, selected.size());

        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \  \   \
        //           2       4       1  3   5   --level 2
        //         /\  \    / \  \    \  \   \
        //       2  12 22  4  14  24   1  3  5  --level 3
        LOGGER.info("-------------------------");
        storages = buildSet(even_rack_2[0], even_rack_2[1], even_rack_2[2], even_rack_4[0], even_rack_4[1], even_rack_4[2], odd_rack_1[0], odd_rack_3[0],odd_rack_5[0]);
        selected = policy.pruneAtLevel(storages, 2);
        Assert.assertEquals("prune for level 2 with exceeded balanced nodes", 3, selected.size());
    }
}
