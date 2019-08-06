package com.sf.hadoop;

import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.UnresolvedTopologyException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.DNSToSwitchMapping;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DNSToSwitchMappingReloadServicePlugin extends ReconfigurableServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(DNSToSwitchMappingReloadServicePlugin.class);

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
    protected void doReconfigurate(HttpServletRequest request) {
        LOGGER.info("reload rack resolver :" + this.mapping + " ...");
        DNSToSwitchMapping mapping = this.mapping;
        if (mapping == null) {
            return;
        }

        if (service instanceof NameNode) {
            NameNode namenode = (NameNode) service();
            DatanodeManager manager = namenode.getNamesystem()
                    .getBlockManager()
                    .getDatanodeManager();

            // trigger evaluation
            NamespaceInfo namespace = namenode.getFSImage().getStorage().getNamespaceInfo();
            List<DatanodeRegistration> registrations = manager.getDatanodeListForReport(HdfsConstants.DatanodeReportType.LIVE).parallelStream()
                    // reload mapping
                    .peek((datanode) -> Stream.of(
                            datanode.getIpAddr(),
                            datanode.getHostName()
                            ).parallel()
                                    .map(Collections::singletonList)
                                    .peek(mapping::reloadCachedMappings)
                                    .forEach(mapping::resolve)
                    )
                    .map((datanode) -> new DatanodeRegistration(
                                    datanode,
                                    new StorageInfo(
                                            HdfsConstants.DATANODE_LAYOUT_VERSION,
                                            namespace.getNamespaceID(),
                                            namespace.getClusterID(),
                                            namespace.getCTime(),
                                            HdfsServerConstants.NodeType.DATA_NODE
                                    ),
                                    new ExportedBlockKeys(),
                                    datanode.getSoftwareVersion()
                            )
                    )
                    .collect(Collectors.toList());

            // finner lock acquisition
            FSNamesystem namesystem = namenode.getNamesystem();
            try {
                namesystem.writeLock();
                for (DatanodeRegistration registration : registrations) {
                    try {
                        manager.registerDatanode(registration);
                    } catch (UnresolvedTopologyException | DisallowedDatanodeException e) {
                        LOGGER.warn("refresh datanode fail:" + registration);
                    }
                }
            } catch (Throwable throwable) {
                LOGGER.warn("unexpected exception", throwable);
            } finally {
                namesystem.writeUnlock();
            }
        }
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

    @Override
    protected String render() {
        NameNode namenode = (NameNode) service();
        return new GsonBuilder()
                .setPrettyPrinting()
                .create()
                .toJson(namenode.getNamesystem()
                        .getBlockManager()
                        .getDatanodeManager()
                        .getDatanodeListForReport(HdfsConstants.DatanodeReportType.LIVE)
                        .stream()
                        .map((datanode) ->
                                Stream.of(
                                        datanode.getIpAddr(),
                                        datanode.getHostName(),
                                        datanode.getNetworkLocation()
                                ).collect(Collectors.toList())
                        )
                        .collect(Collectors.toList())
                );
    }
}
