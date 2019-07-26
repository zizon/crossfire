package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fs.misc.LazyIterators;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageCluster {

    public static final Log LOGGER = LogFactory.getLog(StorageCluster.class);

    public static String index(Node node) {
        if (node == null) {
            return null;
        }
        return node.getNetworkLocation() + "/" + node.getName();
    }

    public class StorageNode {
        protected final Node node;

        protected final Set<String> children;
        protected final String index;
        protected final String parent_index;


        public StorageNode(Node node) {
            this.node = node;
            this.children = new HashSet<>();
            this.index = StorageCluster.index(node);
            this.parent_index = StorageCluster.index(node.getParent());
        }

        public String index() {
            return this.index;
        }

        public String parentIndex() {
            return parent_index;
        }

        public Node node() {
            return node;
        }

        public void trackChild(String index) {
            this.children.add(index);
        }

        public StorageCluster cluster() {
            return StorageCluster.this;
        }

        public Set<String> children() {
            return this.children;
        }

        public Stream<String> leaves() {
            return this.children.stream()
                    .map(StorageCluster.this::find)
                    .flatMap((child) -> {
                        if (child.children().isEmpty()) {
                            return Stream.of(child.index());
                        }

                        return child.leaves();
                    });
        }

        @Override
        public String toString() {
            return String.format(
                    "index:%s children:%d",
                    index,
                    children.size()
            );
        }
    }

    protected final Map<String, StorageNode> indexes;
    protected final StorageNode root;

    public StorageCluster(NetworkTopology topology, Collection<Node> nodes) {
        this.indexes = new HashMap<>();
        nodes.stream()
                // generate nodes
                .flatMap((node) -> LazyIterators.stream(
                        LazyIterators.generate(
                                node,
                                Optional::ofNullable,
                                (context, ignore) -> context.getParent()
                        )
                ))
                // index node
                .map((node) -> this.indexes.computeIfAbsent(
                        index(node),
                        (key) -> new StorageNode(node))
                )
                // find non root
                .filter((storage) -> storage.parentIndex() != null)
                // index parent
                .forEach((storage) -> this.indexes.computeIfAbsent(
                        storage.parentIndex(),
                        (key) -> new StorageNode(storage.node().getParent()))
                        .trackChild(storage.index())
                );

        this.root = this.find(index(topology.getNode(NodeBase.ROOT)));
    }

    public Set<String> children() {
        return this.root.children;
    }

    public StorageNode find(String index) {
        return this.indexes.get(index);
    }

    public StorageNode root() {
        return this.root;
    }
}
