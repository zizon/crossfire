package com.sf.hadoop;

import com.fs.misc.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Reconfigurable;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.util.ServicePlugin;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class NameNodeServicePlugin implements ServicePlugin, Reconfigurable {

    public static final Log LOGGER = LogFactory.getLog(NameNodeServicePlugin.class);

    protected static class NameNodeContext {
        protected final NameNode naemnode;
        protected final Promise<Configuration> configuration;
        protected final Promise<DNSToSwitchMapping> resolver;

        public NameNodeContext(NameNode naemnode) {
            this.naemnode = naemnode;

            this.configuration = Promise.light(() -> {
                Field field = NameNode.class.getDeclaredField("conf");
                field.setAccessible(true);
                return (Configuration) field.get(naemnode);
            });

            this.resolver = Promise.light(() -> {
                Field field = DatanodeManager.class.getDeclaredField("dnsToSwitchMapping");
                field.setAccessible(true);

                return (DNSToSwitchMapping) field.get(naemnode.getNamesystem().getBlockManager().getDatanodeManager());
            });
        }

        public Promise<Configuration> configuration() {
            return configuration;
        }

        public Promise<DNSToSwitchMapping> resolver() {
            return this.resolver;
        }

    }

    protected NameNodeContext namenode;
    protected Queue<Promise<?>> scheduling = new ConcurrentLinkedQueue<>();

    @Override
    public void start(Object service) {
        this.namenode = new NameNodeContext((NameNode) service);

        // cancel first
        this.cancelAll();

        // start reconfiguration agent
        LOGGER.info("start NamenodeServicePlugin reconfiguration agent with 5 sec reload window");
        Promise<Boolean> reload_configuration = namenode.configuration().transform((configuration) -> {
            this.schedule(() -> {
                LOGGER.info("trigger NamenodeServicePlugin configuration reload...");
                StreamSupport.stream(new HdfsConfiguration().spliterator(), true)
                        .filter((entry) -> this.isPropertyReconfigurable(entry.getKey()))
                        .filter((entry) -> {// find new config value
                                    return Optional.ofNullable(configuration.get(entry.getKey(), null))
                                            // filter changed
                                            .map((old) -> old.compareTo(entry.getValue()) != 0)
                                            // if not presented?
                                            .orElse(Objects.nonNull(entry.getValue()));
                                }
                        )
                        .forEach((entry) -> this.reconfigureProperty(entry.getKey(), entry.getValue()));
            });

            return true;
        });


        // start dns switch map refresh
        Promise<Boolean> reload_resolver = namenode.resolver().transform((resolver) -> {
            this.schedule(resolver::reloadCachedMappings);
            return true;
        });

        // trigger excetpion if any
        Promise.all(reload_configuration, reload_resolver).join();
    }

    @Override
    public void stop() {
        this.cancelAll();
    }

    @Override
    public void close() {
        this.stop();
    }

    @Override
    public String reconfigureProperty(String property, String newVal) {
        return null;
    }

    @Override
    public boolean isPropertyReconfigurable(String property) {
        return false;
    }

    @Override
    public Collection<String> getReconfigurableProperties() {
        return Collections.emptyList();
    }

    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public Configuration getConf() {
        return this.namenode.configuration().join();
    }

    protected void cancelAll() {
        // cancel all
        for (; ; ) {
            boolean more = Optional.ofNullable(scheduling.poll())
                    .map((scheudled) -> {
                        scheudled.cancel(true);
                        return true;
                    }).orElse(false);

            if (!more) {
                break;
            }
        }
    }

    protected Promise<?> schedule(Promise.PromiseRunnable runnable) {
        Promise<?> scheduled = Promise.period(runnable, TimeUnit.MINUTES.toMillis(1));

        // marked
        scheduling.offer(scheduled);

        return scheduled;
    }
}
