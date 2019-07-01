package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ActiveZoneDNSToSwitchMapping implements DNSToSwitchMapping, Configurable {

    public static final Log LOGGER = LogFactory.getLog(ActiveZoneDNSToSwitchMapping.class);

    public static final String TOPOLOGY_FILE = "com.sf.topology.file.path";

    protected Configuration configuration;

    protected ConcurrentMap<String, Map.Entry<String, Long>> lookup_cache = new ConcurrentHashMap<>();

    @Override
    public List<String> resolve(List<String> names) {
        return names.parallelStream().map((name) -> {
            return lookup_cache.compute(name, (ignore, old) -> {
                if (old == null) {
                    old = new AbstractMap.SimpleEntry<>(NetworkTopology.DEFAULT_RACK, System.currentTimeMillis());
                }

                // update access time
                old.setValue(System.currentTimeMillis());
                return old;
            }).getKey();
        }).collect(Collectors.toList());
    }

    @Override
    public void reloadCachedMappings() {
        Optional.of(this.configuration.get(TOPOLOGY_FILE))
            .map(URI::create)
            .ifPresent((file) -> {
                try (BufferedReader reader = new BufferedReader(new FileReader(new File(file)))) {
                    reader.lines().parallel().forEach(this::consumeLine);
                } catch (IOException e) {
                    LOGGER.error("fail to load topology:" + file, e);
                }
            });

        expireCache();
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
        this.reloadCachedMappings();
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
        this.reloadCachedMappings();
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    protected void consumeLine(String line) {
        Iterator<String> parts = Arrays.stream(line.replaceAll("\t", " ").split(" "))
            .parallel()
            .map(String::trim)
            .filter((string) -> !string.isEmpty())
            .map(String::toLowerCase)
            .iterator();

        // find host name
        if (!parts.hasNext()) {
            return;
        }
        String ip = NetUtils.normalizeHostName(parts.next());

        // no location
        if (!parts.hasNext()) {
            lookup_cache.put(ip, new AbstractMap.SimpleEntry<>(NetworkTopology.DEFAULT_RACK, System.currentTimeMillis()));
            return;
        }

        // set location
        String location = parts.next();
        if (location.startsWith("/")) {
            lookup_cache.put(ip, new AbstractMap.SimpleEntry<>(location, System.currentTimeMillis()));
        } else {
            lookup_cache.put(ip, new AbstractMap.SimpleEntry<>("/" + location, System.currentTimeMillis()));
        }

        return;
    }

    protected void expireCache() {
        long now = System.currentTimeMillis();
        lookup_cache.entrySet().parallelStream()
            .forEach((entry) -> {
                if (TimeUnit.MILLISECONDS.toMinutes(now - entry.getValue().getValue()) > 5) {
                    lookup_cache.remove(entry.getKey());
                }
            });
    }
}
