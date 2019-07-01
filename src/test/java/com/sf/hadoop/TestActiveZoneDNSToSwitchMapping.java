package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.Test;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

public class TestActiveZoneDNSToSwitchMapping {

    public static Log LOGGER = LogFactory.getLog(TestActiveZoneDNSToSwitchMapping.class);

    @Test
    public void test() throws Throwable {
        URL file = Thread.currentThread().getContextClassLoader().getResource("rack.data");
        Configuration configuration = new Configuration();
        configuration.set(ActiveZoneDNSToSwitchMapping.TOPOLOGY_FILE, file.toExternalForm());

        ActiveZoneDNSToSwitchMapping mapping = new ActiveZoneDNSToSwitchMapping();
        mapping.setConf(configuration);

        Arrays.stream(new String[]{
            "10.116.100.1",
            "10.15.15.14",
            "localhost",
            "cnsz17pl1784"
        }).forEach((ip) -> {
            LOGGER.info("resolving " + ip + " to:" + mapping.resolve(Collections.singletonList(ip)));
        });

    }
}
