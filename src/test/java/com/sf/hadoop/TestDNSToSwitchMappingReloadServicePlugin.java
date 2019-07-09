package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestDNSToSwitchMappingReloadServicePlugin {

    public static Log LOGGER = LogFactory.getLog(TestDNSToSwitchMappingReloadServicePlugin.class);

    protected static Path RACK_DATA = Paths.get("rack.data");
    protected static List<String> TEST_SET = Stream.of(
            "10.116.100.1    /other/V01",
            "10.116.100.2    V02",
            "10.116.100.3    AA08",
            "10.116.100.4    AA09",
            "10.116.100.5    V05",
            "10.116.100.6    V06",
            "10.116.100.7    /remote/AC15",
            "10.116.100.8    AC16",
            "10.116.100.9    AC18",
            "10.116.100.10   AC17",
            "10.116.100.11   AC19",
            "10.116.100.12   AC20",
            "10.116.100.13   V01",
            "10.116.100.14   V01",
            "10.116.100.15   V01",
            "10.116.100.16   V01",
            "10.116.100.17   V01",
            "10.116.100.18   V01",
            "10.116.100.19   V01",
            "10.116.100.19   V01"
    ).collect(Collectors.toList());

    @Before
    public void setup() throws Throwable {
        ByteBuffer rack = ByteBuffer.wrap(
                TEST_SET.stream()
                        .collect(Collectors.joining("\n"))
                        .getBytes()
        );

        try (FileChannel channel = FileChannel.open(RACK_DATA, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            channel.map(FileChannel.MapMode.READ_WRITE, 0, rack.limit()).put(rack);
        }
    }

    @After
    public void tearup() throws Throwable {
        // trigger unmap
        System.gc();
        Files.deleteIfExists(RACK_DATA);
    }

    @Test
    public void test() throws Throwable {
        Configuration configuration = new Configuration();
        //configuration.set(DNSToSwitchMappingReloadServicePlugin.TOPOLOGY_FILE, RACK_DATA.toUri().toURL().toExternalForm());

        DNSToSwitchMappingReloadServicePlugin mapping = new DNSToSwitchMappingReloadServicePlugin(TimeUnit.SECONDS.toMillis(5));
        mapping.setConf(configuration);
        mapping.start(null);

        Arrays.stream(new String[]{
                "10.116.100.1",
                "10.15.15.14",
                "localhost",
                "cnsz17pl1784",
                "10.116.100.7"
        }).forEach((ip) -> {
            //LOGGER.info("resolving " + ip + " to:" + mapping.resolve(Collections.singletonList(ip)));
        });

        LOGGER.info( DatanodeManager.class.getDeclaredField("dnsToSwitchMapping"));
        //LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(15));
        mapping.stop();
    }
}
