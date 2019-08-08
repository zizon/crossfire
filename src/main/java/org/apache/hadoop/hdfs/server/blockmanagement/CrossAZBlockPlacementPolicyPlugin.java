package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.gson.GsonBuilder;
import com.sf.hadoop.DNSToSwitchMappingReloadServicePlugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.log.LogLevel;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CrossAZBlockPlacementPolicyPlugin extends DNSToSwitchMappingReloadServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(CrossAZBlockPlacementPolicyPlugin.class);

    public static String RESOLVE_SCRIPT = "com.sf.resolve-script";

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
    protected CrossAZBlockPlacementPolicy crossaz_policy;
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

        if (this.mapping instanceof Configurable) {
            Optional.ofNullable(this.configuration.get(RESOLVE_SCRIPT))
                    .ifPresent((script) -> configuration.set(
                            CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                            script)
                    );

            ((Configurable) this.mapping).setConf(configuration);
        }

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
                .orElseGet(() -> namenode.getNamesystem()
                        .getBlockManager()
                        .getBlockPlacementPolicy()
                        != crossaz_policy
                );
        if (fallback) {
            disableCrossAZBlockPlacementPolicy();
        } else {
            enableCrossAZBlockPlacementPolicy();
        }

        boolean fast_verify = Optional.ofNullable(request.getParameter("fast-verify"))
                .map(Boolean::parseBoolean)
                .orElseGet(crossaz_policy::isFastVerifyEnable);
        crossaz_policy.setFastVerify(fast_verify);
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

        // current fast verify state
        content.put("fast-verify", crossaz_policy.isFastVerifyEnable());

        return new GsonBuilder()
                .setPrettyPrinting()
                .create()
                .toJson(content);
    }
}
