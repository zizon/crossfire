package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            namenode.getNamesystem()
                    .getBlockManager()
                    .getDatanodeManager()
                    .getDatanodeListForReport(HdfsConstants.DatanodeReportType.LIVE)
                    .parallelStream()
                    .forEach((datanode) -> {
                        String location = Stream.of(
                                datanode.getIpAddr(),
                                datanode.getHostName()
                        ).map((name) -> mapping.resolve(Collections.singletonList(name)))
                                .filter((resolved) -> resolved != null && !resolved.isEmpty())
                                .map((list) -> list.get(0))
                                .findAny()
                                .orElse(NetworkTopology.DEFAULT_RACK);

                        LOGGER.info("update dateanode:" + datanode + " to location:" + location);
                        datanode.setNetworkLocation(location);
                    });
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
