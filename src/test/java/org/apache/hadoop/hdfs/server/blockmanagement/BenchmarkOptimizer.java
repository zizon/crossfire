package org.apache.hadoop.hdfs.server.blockmanagement;


import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.Node;
import org.junit.Before;
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

import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkOptimizer {

    protected CrossAZBlockPlacementPolicy.Optimizer optimizer;
    protected List<DatanodeInfo> datanodes;
    protected NavigableSet<Node> nodes;

    @Setup
    public void setup() {
        this.optimizer = new CrossAZBlockPlacementPolicy.Optimizer();
        this.datanodes = IntStream.range(1, 255)
                .mapToObj((i) -> {
                    String datacenter = i % 2 == 0 ? "even" : "odd";
                    String rack = "rack_" + i % 10;
                    String location = "/" + datacenter + "/" + rack;
                    String ip = "10.202.77." + i;
                    String hostname = "datanode_" + i;
                    DatanodeID id = new DatanodeID(ip, hostname, UUID.randomUUID().toString(), 0, 0, 0, 0);
                    return new DatanodeInfo(id, location);
                }).collect(Collectors.toList());

        DatanodeInfo[] even_rack_2 = selectSubset("even", "rack_2");
        DatanodeInfo[] odd_rack_1 = selectSubset("odd", "rack_1");

        nodes = CrossAZBlockPlacementPolicy.newNodeSet();
        nodes.add(odd_rack_1[0]);
        nodes.add(odd_rack_1[1]);
        nodes.add(even_rack_2[0]);
        nodes.add(even_rack_2[1]);
        nodes.add(even_rack_2[2]);
        nodes.add(even_rack_2[3]);
    }

    protected DatanodeInfo[] selectSubset(String datacenter, String rack) {
        return datanodes.stream()
                .filter((datanode) -> datanode.getNetworkLocation().contains(datacenter))
                .filter((datanode) -> datanode.getNetworkLocation().contains(rack))
                .toArray(DatanodeInfo[]::new);
    }

    @Benchmark
    public void verfiy() {
        optimizer.isOptimal(nodes, 5);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkOptimizer.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
