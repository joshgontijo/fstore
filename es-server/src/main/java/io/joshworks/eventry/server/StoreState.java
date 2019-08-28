package io.joshworks.eventry.server;

import io.joshworks.eventry.server.cluster.Node;
import io.joshworks.fstore.es.shared.Status;
import io.joshworks.fstore.es.shared.streams.StreamHasher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StoreState {

    private final Map<String, Node> nodes = new ConcurrentHashMap<>();
    private final Node[] partitions;

    public StoreState(int numPartitions) {
        this.partitions = new Node[numPartitions];
    }

    public void assign(int partition, Node node) {
        partitions[partition] = node;
    }

    public Node select(long stream) {
        return partitions[(int) (stream % partitions.length)];
    }

    public Node select(String stream) {
        return select(StreamHasher.hash(stream));
    }

    public void addNode(Node node, Set<Integer> partitions) {
        nodes.put(node.id, node);
        for (Integer pid : partitions) {
            this.partitions[pid] = node;
        }
    }

    public void updateNode(String nodeId, Status status) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Node not found for id " + nodeId);
        }
        node.status = status;
    }

    public Node getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public Set<Integer> nodePartitions(String nodeId) {
        return IntStream.range(0, partitions.length)
                .filter(i -> partitions[i] != null)
                .filter(i -> nodeId.equals(partitions[i].id))
                .boxed()
                .collect(Collectors.toSet());
    }

    public List<Node> nodes() {
        return new ArrayList<>(nodes.values());
    }
}
