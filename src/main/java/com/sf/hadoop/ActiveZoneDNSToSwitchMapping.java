package com.sf.hadoop;

import com.fs.misc.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNSToSwitchMapping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ActiveZoneDNSToSwitchMapping implements DNSToSwitchMapping, Configurable {

    public static final Log LOGGER = LogFactory.getLog(ActiveZoneDNSToSwitchMapping.class);

    public static final String TOPOLOGY_FILE = "com.sf.topology.file.path";

    protected static class Topology {

        protected final String location;
        protected final Collection<String> hierarchy;

        public Topology(Collection<String> hierarchy) {
            this.hierarchy = hierarchy;
            this.location = "/" + hierarchy.parallelStream().collect(Collectors.joining("/"));
        }

        public String location() {
            return location;
        }

        @Override
        public String toString() {
            return this.location;
        }

    }

    protected static class Topologys {
        protected static final String DEFAULT_GROUP = "default";

        protected final ConcurrentMap<String, Topology> lookup_cache;
        protected final int max_depth;
        protected final String default_resolved;

        public Topologys(ConcurrentMap<String, Deque<String>> raw_lookup_cache, int max_depth) {
            this.max_depth = max_depth;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("apply topology:"
                        + raw_lookup_cache.entrySet().parallelStream()
                        .map((entry) ->
                                ")ip:" + entry.getKey()
                                        + " raw location:[" + entry.getValue().stream().collect(Collectors.joining(",")) + "])"
                        )
                        .collect(Collectors.joining(", ")).toString()
                        + " max depth:" + max_depth);
            }

            // align topology to match depth
            this.lookup_cache = raw_lookup_cache.entrySet().parallelStream()
                    .map((entry) -> {
                        Deque<String> topology = entry.getValue();
                        int size = topology.size();

                        // fill missing
                        IntStream.range(size, max_depth - size + 1)
                                .forEach((ignore) -> {
                                    topology.addFirst(DEFAULT_GROUP);
                                });

                        // remap
                        Map.Entry<String, Topology> remaped = new AbstractMap.SimpleImmutableEntry<>(
                                entry.getKey(),
                                new Topology(topology)
                        );

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("align ip:" + remaped.getKey() + " topology:" + remaped.getValue());
                        }

                        return remaped;
                    })
                    .collect(Collectors.toConcurrentMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));

            // default location
            this.default_resolved = "/" + IntStream.range(0, max_depth)
                    .mapToObj((ignore) -> DEFAULT_GROUP)
                    .collect(Collectors.joining("/"));

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("default resolve:" + this.default_resolved + " default group:" + DEFAULT_GROUP);
            }
        }

        public String resolve(String name) {
            return Optional.ofNullable(this.lookup_cache.getOrDefault(name, null))
                    .map(Topology::location)
                    .orElse(default_resolved);
        }
    }

    protected Configuration configuration;

    protected Promise<Topologys> topologys = Promise.light(this::loadCachedMappings);

    @Override
    public List<String> resolve(List<String> names) {
        return topologys.transform((lookup_cache) ->
                names.parallelStream()
                        .map(lookup_cache::resolve)
                        .collect(Collectors.toList())
        ).join();
    }

    @Override
    public void reloadCachedMappings() {
        Promise<Topologys> old = topologys;
        this.topologys = Promise.light(this::loadCachedMappings).fallback(old::join);
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

    protected Topologys loadCachedMappings() {
        return Optional.ofNullable(this.configuration.get(TOPOLOGY_FILE))
                .map(URI::create)
                .map((file) -> {
                    // open file
                    ConcurrentMap<String, Deque<String>> lookup = new ConcurrentHashMap<>();
                    try (BufferedReader reader = new BufferedReader(new FileReader(new File(file)))) {
                        // 1. transform line to parts
                        // 2. build lookup
                        // 3. calculate max topology depth
                        int max_depth = reader.lines().parallel().map((line) ->
                                Arrays.stream(line.replaceAll("\t", " ").split(" "))
                                        .parallel()
                                        .map(String::trim)
                                        .filter((string) -> !string.isEmpty())
                                        .map(String::toLowerCase)
                                        .collect(Collectors.toList())
                        ).filter((parts) -> parts.size() >= 2)
                                .map((parts) -> {
                                    // slice ip and topology
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("parse raw topology:" + parts);
                                    }

                                    String key = parts.get(0);
                                    String value = parts.get(1);

                                    lookup.put(
                                            key,
                                            Optional.ofNullable(value)
                                                    .map((non_null) ->
                                                            Arrays.stream(non_null.split("/"))
                                                                    .map(String::trim)
                                                                    .filter((cell) -> !cell.isEmpty())
                                                                    .collect(Collectors.toCollection(ConcurrentLinkedDeque::new))
                                                    )
                                                    .orElseGet(ConcurrentLinkedDeque::new)
                                    );

                                    // side effect,calculate size
                                    return parts.size();
                                })
                                .max(Integer::compareTo)
                                .orElse(1);

                        return new Topologys(lookup, max_depth);
                    } catch (IOException e) {
                        LOGGER.error("fail to load topology:" + file, e);
                        return null;
                    }
                })
                .orElseGet(() -> new Topologys(new ConcurrentHashMap<>(), 1))
                ;
    }
}
