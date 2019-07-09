package com.sf.hadoop;

import com.fs.misc.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ServicePlugin;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class ReconfigurableServicePlugin implements Configurable, ServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(ReconfigurableServicePlugin.class);

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
    }

    protected Object service;
    protected Promise<?> scheduling;
    protected Configuration configuration;
    protected final NavigableSet<String> listening_property;
    protected long reload_interval;

    public ReconfigurableServicePlugin(NavigableSet<String> listening_propert, long reload_interval) {
        super();
        this.listening_property = Optional.ofNullable(listening_propert).orElseGet(ConcurrentSkipListSet::new);
        this.reload_interval = reload_interval;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    @Override
    public void start(Object service) {
        LOGGER.info("start service plugin:" + this + " for service:" + service);
        this.cancel();
        this.service = service;
        this.scheduling = Promise.period(this::maybeReconfigurate, this.reload_interval);
    }

    @Override
    public void stop() {
        LOGGER.info("stop service plugin:" + this + " of service:" + service());
        this.cancel();
        this.scheduling = null;
        this.service = null;
    }

    @Override
    public void close() {
        this.stop();
    }

    protected Object service() {
        return service;
    }

    protected void cancel() {
        Optional.ofNullable(this.scheduling)
                .ifPresent((promise) -> promise.cancel(true));
    }

    protected void changeInterval(long period) {
        this.stop();
        this.reload_interval = period;
        this.start(service());
    }

    protected abstract void notifyChangeSet(Configuration old_config, Configuration new_config, ChangeSet changeset);

    protected void maybeReconfigurate() {
        // find current
        Configuration current = this.getConf();

        // load new
        Configuration maybe_new = new Configuration(current);
        maybe_new.reloadConfiguration();

        ChangeSet changeset = new ChangeSet();
        this.listening_property.parallelStream()
                .forEach((property) -> {
                    String old_value = current.get(property, null);
                    String new_value = maybe_new.get(property, null);

                    if (old_value == null) {
                        if (new_value != null) {
                            changeset.added(property);
                        }

                        // not changed
                    } else if (new_value == null) {
                        // remove
                        changeset.removed(property);
                    } else if (old_value.compareTo(new_value) != 0) {
                        changeset.modified(property);
                    }
                });

        // update new config
        this.setConf(maybe_new);

        // do notify
        this.notifyChangeSet(current, maybe_new, changeset);
    }
}
