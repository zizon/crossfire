package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageCluster {

    public static final Log LOGGER = LogFactory.getLog(StorageCluster.class);

    public static class StorageNode {
        protected final Node node;
        protected StorageNode parent;
        protected final Map<String, StorageNode> children;
        protected final String name;

        public StorageNode(Node node) {
            this.node = node;
            this.children = new HashMap<>();
            this.name = node.getNetworkLocation() + "/" + node.getName();
        }

        public String name() {
            return this.name;
        }

        public StorageNode parent() {
            return parent;
        }

        public Map<String, StorageNode> children() {
            return children;
        }

        public StorageNode addChild(StorageNode child) {
            this.children.compute(child.name, (name, old) -> {
                if (old != null) {
                    old.merge(child);
                    return old;
                }

                child.parent = this;
                return child;
            });

            return this;
        }

        public StorageNode merge(StorageNode other) {
            for (StorageNode node:other.children.values()){
                this.addChild(node);
            }
            return this;
        }

        public Set<StorageNode> leaves() {
            return this.children.values().stream()
                    .flatMap((child) -> {
                        if (child.children().size() == 0) {
                            return Stream.of(child);
                        }

                        return child.leaves().stream();
                    })
                    .collect(Collectors.toSet());
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof StorageNode) {
                return this.name.equals(((StorageNode) o).name);
            }

            return false;
        }

        @Override
        public String toString() {
            return String.format(
                    "(location:%s,level:%d,name:%s,children:%d)",
                    node.getNetworkLocation(),
                    node.getLevel(),
                    node.getName(),
                    children.size()
            );
        }
    }

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
            for (; ; ) {
                Node node_parent = storage.node.getParent();
                if (node_parent == null) {
                    // reach root
                    this.root.merge(storage);
                    break;
                }

                storage = new StorageNode(node_parent).addChild(storage);
            }
        }
    }

    public Map<String, StorageNode> children() {
        return this.root.children;
    }

    public StorageNode root() {
        return root;
    }
}
