package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.gson.GsonBuilder;
import com.sf.hadoop.DNSToSwitchMappingReloadServicePlugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.util.ServletUtil;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.swing.text.html.Option;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CrossAZBlockPlacementPolicyPlugin extends DNSToSwitchMappingReloadServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(CrossAZBlockPlacementPolicyPlugin.class);

    public static class LogServlet extends LogLevel.Servlet {

        @Override
        public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
            Configuration conf = (Configuration) this.getServletContext()
                    .getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE);

            boolean security_on = conf.getBoolean(
                    CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
                    false
            );

            if (security_on) {
                // security enable, temporary disable
                conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
            }

            try {
                super.doGet(request, response);
            } catch (IOException | ServletException exception) {
                throw new IOException(exception);
            } finally {
                // re-enable
                if (security_on) {
                    conf.setBoolean(
                            CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
                            true
                    );
                }
            }
        }
    }

    protected NameNode namenode;
    protected Configuration configuration;
    protected BlockPlacementPolicy default_policy;
    protected BlockPlacementPolicy crossaz_policy;
    protected MethodHandle policy_settter;

    @Override
    public void start(Object service) {
        super.start(service);

        if (!(service instanceof NameNode)) {
            throw new IllegalArgumentException(String.format(
                    "service:%s should be instance of %s",
                    service,
                    NameNode.class.getName()
            ));
        }

        this.namenode = (NameNode) service;
        this.configuration = stealNamenodeConfiguration();

        // special hack for log level config,
        // to bypass security check
        if (configuration.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
            http().addServlet("logLevel", "/logLevel", LogServlet.class);
        }

        // learn and setup policy
        BlockManager block_manager = namenode.getNamesystem().getBlockManager();
        this.default_policy = block_manager.getBlockPlacementPolicy();
        this.crossaz_policy = new CrossAZBlockPlacementPolicy();
        DatanodeManager datanode_manager = block_manager.getDatanodeManager();
        this.crossaz_policy.initialize(
                this.configuration,
                datanode_manager.getFSClusterStats(),
                datanode_manager.getNetworkTopology(),
                datanode_manager.getHost2DatanodeMap()
        );

        String policy_field_name = "blockplacement";
        try {
            Field policy_field = BlockManager.class.getDeclaredField(policy_field_name);
            policy_field.setAccessible(true);
            policy_settter = MethodHandles.lookup().unreflectSetter(policy_field)
                    .bindTo(block_manager);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalArgumentException(String.format(
                    "fail to get block placment policy settter:%s",
                    policy_field_name
            ), e);
        }

        // setup getter and setter

        // enable placement policy
        enableCrossAZBlockPlacementPolicy();
    }

    protected Configuration stealNamenodeConfiguration() {
        try {
            Field field = NameNode.class.getDeclaredField("conf");
            field.setAccessible(true);
            return (Configuration) field.get(namenode);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "fail to get namenode conf, expected field:conf",
                    e
            );
        }
    }

    protected void enableCrossAZBlockPlacementPolicy() {
        try {
            LOGGER.info(String.format(
                    "switch to policy:%s",
                    crossaz_policy
            ));
            policy_settter.invoke(crossaz_policy);
        } catch (Throwable throwable) {
            LOGGER.error(String.format(
                    "fail to set block placement policy to:%s current:%s",
                    crossaz_policy,
                    namenode.getNamesystem().getBlockManager().getBlockPlacementPolicy()
            ));
        }
    }

    protected void disableCrossAZBlockPlacementPolicy() {
        try {
            LOGGER.info(String.format(
                    "switch to policy:%s",
                    default_policy
            ));
            policy_settter.invoke(default_policy);
        } catch (Throwable throwable) {
            LOGGER.error(String.format(
                    "fail to set block placement policy to:%s current:%s",
                    default_policy,
                    namenode.getNamesystem().getBlockManager().getBlockPlacementPolicy()
            ));
        }
    }

    @Override
    protected void doReconfigurate(HttpServletRequest request) {
        // forward super
        super.doReconfigurate(request);

        boolean fallback = Optional.ofNullable(request.getParameter("fallback"))
                .map(Boolean::parseBoolean)
                .orElse(false);
        if (fallback) {
            disableCrossAZBlockPlacementPolicy();
        } else {
            enableCrossAZBlockPlacementPolicy();
        }
    }

    @Override
    protected String name() {
        return "crossaz";
    }

    @Override
    protected String render() {
        Map<String, Object> content = new TreeMap<>();

        // set datandoes
        content.put("datanodes", namenode.getNamesystem()
                .getBlockManager()
                .getDatanodeManager()
                .getDatanodeListForReport(HdfsConstants.DatanodeReportType.LIVE)
                .stream()
                .map((datanode) -> {
                    Map<String, String> node = new TreeMap<>();
                    node.put("address", datanode.getXferAddr());
                    node.put("location", datanode.getNetworkLocation());
                    return node;
                })
                .collect(Collectors.toList())
        );

        // set using policy
        content.put("policy", namenode.getNamesystem()
                .getBlockManager()
                .getBlockPlacementPolicy()
                .getClass()
                .getName()
        );

        // fallback flag
        content.put("fallback", namenode.getNamesystem()
                .getBlockManager()
                .getBlockPlacementPolicy() == default_policy
        );

        return new GsonBuilder()
                .setPrettyPrinting()
                .create()
                .toJson(content);
    }
}