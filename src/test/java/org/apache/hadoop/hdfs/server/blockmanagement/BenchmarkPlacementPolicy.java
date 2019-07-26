package org.apache.hadoop.hdfs.server.blockmanagement;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkPlacementPolicy {

    NavigableSet<DatanodeInfo> datanodes;
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
                .collect(Collectors.toCollection(() -> new TreeSet<>(CrossAZBlockPlacementPolicy.NODE_COMPARATOR)));

        policy = new CrossAZBlockPlacementPolicy();
        default_policy = new BlockPlacementPolicyDefault();
        topology = new NetworkTopology();
        datanodes.forEach(topology::add);
        policy.initialize(null, null, topology, null);
        default_policy.initialize(new Configuration(), null, topology, null);

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
                .collect(Collectors.toCollection(() -> new TreeSet<>(CrossAZBlockPlacementPolicy.STORAGE_COMPARATOR)));
    }

    @Benchmark
    public void clusterSetup() {
        StorageCluster cluster = new StorageCluster(topology,topology.getLeaves(NodeBase.ROOT));
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

    @Benchmark
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

    @Benchmark
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

    @Benchmark
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

    @Benchmark
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
                .measurementIterations(60)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
