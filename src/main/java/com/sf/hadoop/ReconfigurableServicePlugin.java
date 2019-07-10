package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Reconfigurable;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationServlet;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.util.ServicePlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class ReconfigurableServicePlugin implements Reconfigurable, ServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(ReconfigurableServicePlugin.class);

    public static final String RECONFIGURATION_KEY = "reconfig";

    public static class ChangeSet {

        protected NavigableSet<String> removed_keys;
        protected NavigableSet<String> added_keys;
        protected NavigableSet<String> modified_keys;

        public void removed(String key) {
            if (removed_keys == null) {
                this.removed_keys = new ConcurrentSkipListSet<>();
            }
            this.removed_keys.add(key);
        }

        public void added(String key) {
            if (this.added_keys == null) {
                this.added_keys = new ConcurrentSkipListSet<>();
            }
            this.added_keys.add(key);
        }

        public void modified(String key) {
            if (this.modified_keys == null) {
                this.modified_keys = new ConcurrentSkipListSet<>();
            }
            this.modified_keys.add(key);
        }

        public NavigableSet<String> removedSet() {
            if (removed_keys == null) {
                return Collections.emptyNavigableSet();
            }
            return removed_keys;
        }

        public NavigableSet<String> addedSet() {
            if (added_keys == null) {
                return Collections.emptyNavigableSet();
            }
            return added_keys;
        }

        public NavigableSet<String> modifiedSet() {
            if (modified_keys == null) {
                return Collections.emptyNavigableSet();
            }
            return modified_keys;
        }

        public boolean didChanged(String key) {
            return Stream.of(
                    modified_keys,
                    removed_keys,
                    added_keys
            ).parallel()
                    .anyMatch((keys) -> keys != null && keys.contains(key));
        }

        @Override
        public String toString() {
            return "added:(" + addedSet().parallelStream().collect(Collectors.joining(",")) + ")"
                    + "removed:(" + removedSet().parallelStream().collect(Collectors.joining(",")) + ")"
                    + "modified:(" + modifiedSet().parallelStream().collect(Collectors.joining(",")) + ")"
                    ;
        }
    }

    protected Object service;
    protected Configuration configuration;
    protected HttpServer2 http;

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;

        // plug flag
        this.configuration.setBoolean(RECONFIGURATION_KEY, false);
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    @Override
    public void start(Object service) {
        this.service = service;
        try {
            this.http = findHttpServer();
            LOGGER.info("plugin find httpserver:" + http);
        } catch (Throwable throwable) {
            throw new RuntimeException("fail to find http server for:" + this, throwable);
        }

        String name = name();
        String path = "/" + name;
        http.addServlet(name, path, ReconfigurationServlet.class);
        http.setAttribute(ReconfigurationServlet.CONF_SERVLET_RECONFIGURABLE_PREFIX + path, this);

        LOGGER.info("plug reconfig servlet with name:" + name
                + " path:" + path
                + " attribute:" + this
                + "(" + ReconfigurationServlet.CONF_SERVLET_RECONFIGURABLE_PREFIX + path + ")"
        );
    }

    @Override
    public void stop() {
        LOGGER.warn("no action for stop");
    }

    @Override
    public void close() {
        this.stop();
    }

    @Override
    public boolean isPropertyReconfigurable(String property) {
        return RECONFIGURATION_KEY.equals(property);
    }

    @Override
    public Collection<String> getReconfigurableProperties() {
        return Collections.singleton(RECONFIGURATION_KEY);
    }

    @Override
    public String reconfigureProperty(String property, String new_value) throws ReconfigurationException {
        try {
            LOGGER.info("trigger reconfig, property:" + property + " old:" + getConf().get(property) + " new:" + new_value);
            this.doReconfigurate();
            return null;
        } catch (Throwable throwable) {
            throw new ReconfigurationException("fail to reconfigrate:" + property, "false", new_value, throwable);
        }
    }

    protected abstract void doReconfigurate() throws Throwable;

    protected abstract String name();

    protected abstract HttpServer2 findHttpServer() throws Throwable;

    protected HttpServer2 http() {
        return http;
    }

    protected Object service() {
        return service;
    }

}
