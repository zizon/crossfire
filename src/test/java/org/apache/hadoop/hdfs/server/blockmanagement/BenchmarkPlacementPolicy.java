package org.apache.hadoop.hdfs.server.blockmanagement;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkPlacementPolicy {

    NavigableSet<DatanodeDescriptor> datanodes;
    CrossAZBlockPlacementPolicy policy;
    BlockPlacementPolicy default_policy;
    List<DatanodeStorageInfo> storages;
    NetworkTopology topology;
    DatanodeInfo[] even;
    DatanodeInfo[] odd;
    DatanodeInfo[] even_rack_2;
    DatanodeInfo[] even_rack_4;
    DatanodeInfo[] even_rack_6;
    DatanodeInfo[] odd_rack_1;
    DatanodeInfo[] odd_rack_3;

    @Setup
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
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(DatanodeDescriptor::getDatanodeUuid))));


        policy = new CrossAZBlockPlacementPolicy();
        default_policy = new BlockPlacementPolicyDefault();
        topology = new NetworkTopology();
        datanodes.forEach(topology::add);

        Configuration configuration = new Configuration();
        configuration.setBoolean(CrossAZBlockPlacementPolicy.USER_FAST_VERIFY, false);
        policy.initialize(configuration, null, topology, null);

        default_policy.initialize(configuration, new FSClusterStats() {

            @Override
            public int getTotalLoad() {
                return 0;
            }

            @Override
            public boolean isAvoidingStaleDataNodesForWrite() {
                return false;
            }

            @Override
            public int getNumDatanodesInService() {
                return datanodes.size();
            }

            @Override
            public double getInServiceXceiverAverage() {
                return 0;
            }
        }, topology, null);

        even = selectSubset("even", "");
        odd = selectSubset("odd", "");
        even_rack_2 = selectSubset("even", "rack_2");
        even_rack_4 = selectSubset("even", "rack_4");
        even_rack_6 = selectSubset("even", "rack_6");
        odd_rack_1 = selectSubset("odd", "rack_1");
        odd_rack_3 = selectSubset("odd", "rack_3");
    }

    protected DatanodeInfo[] selectSubset(String datacenter, String rack) {
        return datanodes.stream()
                .filter((datanode) -> datanode.getNetworkLocation().contains(datacenter))
                .filter((datanode) -> datanode.getNetworkLocation().contains(rack))
                .toArray(DatanodeInfo[]::new);
    }

    protected void helpBenchmarkVerifyBlockPlacement(BlockPlacementPolicy policy, int replica, DatanodeInfo... datanodes) {
        policy.verifyBlockPlacement(datanodes, replica);
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
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(DatanodeStorageInfo::getStorageID))));
    }

    //@Benchmark
    public void chooseTarget() {
        DatanodeStorageInfo[] selected = policy.chooseTarget(null,
                3,
                null,
                Collections.emptyList(),
                true,
                Collections.emptySet(),
                12,
                BlockStoragePolicySuite.createDefaultSuite().getDefaultPolicy()
        );
    }

    //@Benchmark
    public void chooseTargetDefault() {
        default_policy.chooseTarget(null,
                3,
                null,
                new HashSet<>(),
                12,
                new ArrayList<>(),
                BlockStoragePolicySuite.createDefaultSuite().getDefaultPolicy()
        );
    }

    //@Benchmark
    public void chooseTargetWithWriter() {
        policy.chooseTarget(null,
                3,
                datanodes.first(),
                Collections.emptyList(),
                true,
                Collections.emptySet(),
                12,
                BlockStoragePolicySuite.createDefaultSuite().getDefaultPolicy()
        );
    }

    //@Benchmark
    public void chooseTargetDefaultWithWriter() {
        default_policy.chooseTarget(null,
                3,
                datanodes.first(),
                new HashSet<>(),
                12,
                new ArrayList<>(),
                BlockStoragePolicySuite.createDefaultSuite().getDefaultPolicy()
        );
    }

    @Benchmark
    public void verfiyNormal() {
        //             root      --level 0
        //             /   \
        //          even   odd    --level 1
        //         /\ \     / \
        //       2  4 6    1  3    --level 2
        //       |  | |    |  |
        //      (1)(1)(1) (1)(1)   --level 3(leaf)
        helpBenchmarkVerifyBlockPlacement(policy, 5,
                even_rack_2[0], even_rack_4[0], even_rack_6[0], odd_rack_1[0], odd_rack_3[0]
        );
    }

    //@Benchmark
    public void verifyExceed() {
        //             root      --level 0
        //             /   \
        //          even   odd    --level 1
        //         /\      / \
        //       2  4     1  3    --level 2
        //       |  |     |  |
        //      (1)(2)   (1)(1)   --level 3(leaf)
        helpBenchmarkVerifyBlockPlacement(policy, 5,
                even_rack_2[0], even_rack_4[0], even_rack_4[1], odd_rack_1[0], odd_rack_3[0]
        );
    }

    //@Benchmark
    public void verfiyDangle() {
        //             root      --level 0
        //             /   \
        //          even         --level 1
        //         /\
        //       2  4           --level 2
        //       |  |
        //      (2)(3)          --level 3(leaf)
        helpBenchmarkVerifyBlockPlacement(policy, 5,
                even_rack_2[0], even_rack_2[1], even_rack_4[0], even_rack_4[1], even_rack_4[3]
        );
    }

    @Benchmark
    public void verfiyNormalDefault() {
        //             root      --level 0
        //             /   \
        //          even   odd    --level 1
        //         /\ \     / \
        //       2  4 6    1  3    --level 2
        //       |  | |    |  |
        //      (1)(1)(1) (1)(1)   --level 3(leaf)
        helpBenchmarkVerifyBlockPlacement(default_policy, 5,
                even_rack_2[0], even_rack_4[0], even_rack_6[0], odd_rack_1[0], odd_rack_3[0]
        );

    }

    //@Benchmark
    public void verifyExceedDefault() {
        //             root      --level 0
        //             /   \
        //          even   odd    --level 1
        //         /\      / \
        //       2  4     1  3    --level 2
        //       |  |     |  |
        //      (1)(2)   (1)(1)   --level 3(leaf)
        helpBenchmarkVerifyBlockPlacement(default_policy, 5,
                even_rack_2[0], even_rack_4[0], even_rack_4[1], odd_rack_1[0], odd_rack_3[0]
        );
    }

    //@Benchmark
    public void verfiyDangleDefault() {
        //             root      --level 0
        //             /   \
        //          even         --level 1
        //         /\
        //       2  4           --level 2
        //       |  |
        //      (2)(3)          --level 3(leaf)
        helpBenchmarkVerifyBlockPlacement(default_policy, 5,
                even_rack_2[0], even_rack_2[1], even_rack_4[0], even_rack_4[1], even_rack_4[3]
        );
    }

    //@Benchmark
    public void chooseReplicasToDelete() {
        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \
        //           2       4       1    --level 2
        //         /       / \  \    \
        //       2       4  14  24   1  --level 3
        NavigableSet<DatanodeStorageInfo> storages = buildSet(even_rack_2[0], even_rack_4[0], even_rack_4[1], even_rack_4[2], odd_rack_1[0]);
        policy.chooseReplicasToDelete(storages, 3, Collections.emptyList(), null, null);
    }

    //@Benchmark
    public void chooseReplicasToDeleteDefault() {
        //                   root             --level 0
        //                /      \
        //              even      odd         --level 1
        //             /   \        \
        //           2       4       1    --level 2
        //         /       / \  \    \
        //       2       4  14  24   1  --level 3
        NavigableSet<DatanodeStorageInfo> storages = buildSet(even_rack_2[0], even_rack_4[0], even_rack_4[1], even_rack_4[2], odd_rack_1[0]);
        default_policy.chooseReplicasToDelete(storages, 3, Collections.emptyList(), null, null);
    }

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(BenchmarkPlacementPolicy.class.getSimpleName())
                .warmupIterations(0)
                .measurementIterations(2)
                .forks(1)
                .build();
        new Runner(opt).run();

        /*
        BenchmarkPlacementPolicy bechmark = new BenchmarkPlacementPolicy();
        bechmark.setup();

        for(;;){
            bechmark.verfiyNormal();
        }

         */
    }
}
