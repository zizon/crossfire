package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.stream.Collectors;

public class DNSToSwitchMappingReloadServicePlugin extends ReconfigurableServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(DNSToSwitchMappingReloadServicePlugin.class);

    protected DNSToSwitchMapping mapping;

    public static class ResovleResultServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            NameNode namenode = (NameNode) this.getServletContext().getAttribute(ResovleResultServlet.class.getName());
            String rows = namenode.getNamesystem()
                    .getBlockManager()
                    .getDatanodeManager()
                    .getDatanodeListForReport(HdfsConstants.DatanodeReportType.LIVE)
                    .parallelStream()
                    .map((datanode) -> {
                        return "<tr>"
                                + "<td>" + datanode.getIpAddr() + "</td>"
                                + "<td>" + datanode.getHostName() + "</td>"
                                + "<td>" + datanode.getNetworkLocation() + "</td>"
                                + "</tr>";
                    })
                    .collect(Collectors.joining());

            // out
            resp.setContentType("text/html");
            resp.getWriter().print("<!doctype html>"
                    + "<html>"
                    + " <head>"
                    + "     <style>"
                    + "         table {"
                    + "             width: 100%;"
                    + "             text-transform: lowercase;"
                    + "         }"
                    + "     </style>"
                    + " </head>"
                    + " <body>"
                    + "     <table border='1'>"
                    + "          <tbody>"
                    + rows
                    + "         </tbody>"
                    + "     </table>"
                    + " </body>"
                    + "</html>");
        }
    }


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

            // add resolve list servlet
            HttpServer2 http = http();
            http.setAttribute(ResovleResultServlet.class.getName(), namenode);
            http.addServlet("rack-resolved", "/rack-resolved", ResovleResultServlet.class);
        }
    }

    @Override
    protected void doReconfigurate() {
        LOGGER.info("reload rack resolver :" + this.mapping + " ...");
        DNSToSwitchMapping mapping = this.mapping;
        if (mapping == null) {
            return;
        }

        // reload cache
        mapping.reloadCachedMappings();

        if (service instanceof NameNode) {
            NameNode namenode = (NameNode) service();
            DatanodeManager manager = namenode.getNamesystem()
                    .getBlockManager()
                    .getDatanodeManager();

            NamespaceInfo namespace = namenode.getFSImage().getStorage().getNamespaceInfo();
            FSNamesystem namesystem = namenode.getNamesystem();
            for (DatanodeDescriptor datanode : manager.getDatanodeListForReport(HdfsConstants.DatanodeReportType.LIVE)) {
                DatanodeRegistration registration = new DatanodeRegistration(
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
                );

                try {
                    namesystem.writeLock();
                    manager.registerDatanode(registration);
                    LOGGER.info("refresh datanode:" + registration);
                } catch (UnresolvedTopologyException | DisallowedDatanodeException e) {
                    LOGGER.warn("refresh datanode fail:" + registration);
                } finally {
                    namesystem.writeUnlock();
                }
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
}
