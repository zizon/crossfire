package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.yarn.util.RackResolver;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

public class DNSToSwitchMappingReloadServicePlugin extends ReconfigurableServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(DNSToSwitchMappingReloadServicePlugin.class);

    public static final String RELOAD_INTERVAL = "com.sf.rack.resolver.refresh.interval.ms";

    protected List<DNSToSwitchMapping> mappings;

    public DNSToSwitchMappingReloadServicePlugin() {
        this(TimeUnit.MINUTES.toMillis(1));
    }

    public DNSToSwitchMappingReloadServicePlugin(long reload_interval) {
        super(new ConcurrentSkipListSet<String>() {
            {
                this.add(DFSConfigKeys.DFS_NAMENODE_PLUGINS_KEY);
            }
        }, reload_interval);
    }

    @Override
    public void start(Object service) {
        super.start(service);

        List<DNSToSwitchMapping> new_mappings = new ArrayList<>(2);
        RackResolver.init(this.getConf());

        // for rack resolver
        if (service instanceof NameNode) {
            NameNode namenode = (NameNode) service;
            DatanodeManager manager = namenode.getNamesystem().getBlockManager().getDatanodeManager();
            try {
                Field field = DatanodeManager.class.getDeclaredField("dnsToSwitchMapping");
                field.setAccessible(true);
                new_mappings.add((DNSToSwitchMapping) field.get(manager));
            } catch (NoSuchFieldException | IllegalAccessException e) {
                LOGGER.warn("no dnsToSwitchMapping found for namendoe", e);
            }
        }

        try {
            Field field = RackResolver.class.getDeclaredField("dnsToSwitchMapping");
            field.setAccessible(true);
            new_mappings.add((DNSToSwitchMapping) field.get(null));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOGGER.warn("no dnsToSwitchMapping found for rackresolver", e);
        }


        this.mappings = new_mappings;
    }

    @Override
    public void stop() {
        super.stop();
        this.mappings = null;
    }

    protected void reloadCachedMappings() {
        Optional.ofNullable(mappings)
                .ifPresent((mapping) ->
                        mapping.parallelStream()
                                .filter(Objects::nonNull)
                                .forEach(DNSToSwitchMapping::reloadCachedMappings)
                );
    }

    @Override
    protected void notifyChangeSet(Configuration old_config, Configuration new_config, ChangeSet changeset) {
        LOGGER.info("trigger config reload...");
        if (changeset.removedSet().contains(DFSConfigKeys.DFS_NAMENODE_PLUGINS_KEY)) {
            // removed,skip reload
            return;
        }

        // disabled
        if (!getConf().get(DFSConfigKeys.DFS_NAMENODE_PLUGINS_KEY, "").contains(this.getClass().getName())) {
            return;
        }

        if (changeset.modifiedSet().contains(RELOAD_INTERVAL)
                || changeset.addedSet().contains(RELOAD_INTERVAL)
                || changeset.removedSet().contains(RELOAD_INTERVAL)) {
            long interval = getConf().getLong(RELOAD_INTERVAL, TimeUnit.MINUTES.toMillis(1));
            LOGGER.info("update reload interval to:" + interval);
            this.changeInterval(interval);
        }

        // else reload
        LOGGER.info("reload mapping...");
        this.reloadCachedMappings();
    }
}
