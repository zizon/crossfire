package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageCluster {

    public static final Log LOGGER = LogFactory.getLog(StorageCluster.class);

    public static class StorageNode implements Comparable<StorageNode> {
        protected final DatanodeDescriptor datanode;
        protected final Node node;
        protected StorageNode parent;
        protected NavigableSet<StorageNode> children;
        protected NavigableSet<DatanodeStorageInfo> storages;

        public StorageNode(Node node) {
            this.datanode = (DatanodeDescriptor) Optional.ofNullable(node)
                    .filter(datanode -> datanode instanceof DatanodeDescriptor)
                    .orElse(null);
            this.storages = new TreeSet<>(STORAGE_COMPARATOR);

            if (this.datanode != null) {
                for (DatanodeStorageInfo storage : this.datanode.getStorageInfos()) {
                    if (storage.getState() == DatanodeStorage.State.FAILED) {
                        continue;
                    }

                    // add storage
                    this.storages.add(storage);
                }
            }

            this.node = node;
            this.children = new TreeSet<>();
        }

        public StorageNode parent() {
            return parent;
        }

        public NavigableSet<StorageNode> children() {
            return children;
        }

        public StorageNode addChild(StorageNode child) {
            StorageNode hint = this.children.ceiling(child);
            if (hint != null && hint.compareTo(child) == 0) {
                // same, merge it
                hint.merge(child);
            } else {
                // modified parent
                child.parent = this;

                // take over storages
                this.storages.addAll(child.storages);

                // add to children
                this.children.add(child);
            }

            return this;
        }

        public StorageNode merge(StorageNode other) {
            if (compareTo(other) != 0) {
                return this;
            }

            // for storages
            this.storages.addAll(other.storages);

            // merge
            for (StorageNode other_child : other.children()) {
                this.addChild(other_child);
            }

            return this;
        }

        public NavigableSet<StorageNode> leaves() {
            return this.children.stream()
                    .flatMap((child) -> {
                        if (child.children().size() == 0) {
                            return Stream.of(child);
                        }

                        return child.leaves().stream();
                    })
                    .collect(Collectors.toCollection(TreeSet::new));
        }


        @Override
        public int compareTo(StorageNode o) {
            return NODE_COMPARATOR.compare(node, o.node);
        }

        @Override
        public String toString() {
            return String.format(
                    "(location:%s,level:%d,name:%s,children:%d,storages:%d)",
                    node.getNetworkLocation(),
                    node.getLevel(),
                    node.getName(),
                    children.size(),
                    storages.size()
            );
        }
    }

    public static class Storage implements Comparable<Storage> {

        protected final DatanodeStorageInfo storage;
        protected final StorageNode datanode;

        public Storage(DatanodeStorageInfo storage, StorageNode datanode) {
            this.storage = storage;
            this.datanode = datanode;
        }

        @Override
        public int compareTo(Storage o) {
            if (o == null) {
                return 1;
            }

            return this.storage.getStorageID().compareTo(o.storage.getStorageID());
        }
    }

    protected static final Comparator<Node> NODE_COMPARATOR = new Comparator<Node>() {
        final Comparator<Node> comparator = Comparator.comparingInt(Node::getLevel)
                .thenComparing(Node::getName);


        @Override
        public int compare(Node o1, Node o2) {
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                }

                return -1;
            } else if (o2 == null) {
                return 1;
            }

            int compare = o1.getNetworkLocation().compareTo(o2.getNetworkLocation());
            if (compare != 0) {
                return compare;
            }

            return o1.getName().compareTo(o2.getName());
        }
    };

    protected static final Comparator<DatanodeStorageInfo> STORAGE_COMPARATOR = Comparator.comparing(DatanodeStorageInfo::getStorageID);

    protected final StorageNode root;

    public StorageCluster(NetworkTopology topology, Collection<Node> nodes) {
        this.root = new StorageNode(topology.getNode(NodeBase.ROOT));
        if (nodes == null) {
            return;
        }

        for (Node node : nodes) {
            if (node == null) {
                continue;
            }

            StorageNode storage = new StorageNode(node);
            Node tracking = node;
            for (; ; ) {
                Node node_parent = tracking.getParent();
                if (node_parent == null) {
                    // reach root
                    this.root.merge(storage);
                    break;
                }

                storage = new StorageNode(node_parent).addChild(storage);
                tracking = node_parent;
            }
        }
    }

    public NavigableSet<StorageNode> children() {
        return this.root.children;
    }

    public StorageNode root() {
        return root;
    }
}
