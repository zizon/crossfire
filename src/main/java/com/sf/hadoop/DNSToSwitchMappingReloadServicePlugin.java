package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.DNSToSwitchMapping;

import java.lang.reflect.Field;
import java.util.Optional;

public class DNSToSwitchMappingReloadServicePlugin extends ReconfigurableServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(DNSToSwitchMappingReloadServicePlugin.class);

    public static final String RELOAD_INTERVAL = "com.sf.rack.resolver.refresh.interval.ms";

    protected DNSToSwitchMapping mapping;


    @Override
    public void start(Object service) {
        super.start(service);

        // for rack resolver
        if (service instanceof NameNode) {
            NameNode namenode = (NameNode) service;
            DatanodeManager manager = namenode.getNamesystem().getBlockManager().getDatanodeManager();
            try {
                Field field = DatanodeManager.class.getDeclaredField("dnsToSwitchMapping");
                field.setAccessible(true);
                mapping = (DNSToSwitchMapping) field.get(manager);
                LOGGER.info("rack resolver:" + mapping);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                LOGGER.warn("no dnsToSwitchMapping found for namendoe", e);
            }
        }
    }

    @Override
    protected void doReconfigurate() throws Throwable {
        LOGGER.info("reload rack resolver :" + this.mapping + " ...");
        Optional.ofNullable(this.mapping)
                .ifPresent(DNSToSwitchMapping::reloadCachedMappings);
    }

    @Override
    protected String name() {
        return "rack-resolver";
    }

    @Override
    protected HttpServer2 findHttpServer() throws Throwable {
        if (service() instanceof NameNode) {
            // find namenode http server
            Field field = NameNode.class.getDeclaredField("httpServer");
            field.setAccessible(true);
            Object namenode_http_server = field.get(service());

            field = NameNodeHttpServer.class.getDeclaredField("httpServer");
            field.setAccessible(true);
            Object http_server = field.get(namenode_http_server);
            return (HttpServer2) http_server;
        }

        throw new NullPointerException("can not find http server");
    }
}
