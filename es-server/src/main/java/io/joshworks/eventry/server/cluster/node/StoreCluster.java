package io.joshworks.eventry.server.cluster.node;

import io.joshworks.eventry.network.Cluster;
import io.joshworks.eventry.network.ClusterNode;
import io.joshworks.eventry.network.NodeStatus;
import io.joshworks.eventry.network.client.ClusterClient;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StoreCluster {

    private final Cluster cluster;
    private final Partitions partitions;
    private final ClusterClient client;

    public StoreCluster(String clusterName, File root, String nodeId, int numBuckets) {
        partitions = new Partitions(root, nodeId, numBuckets);
        cluster = new Cluster(clusterName, nodeId);
        cluster.onNodeUpdated(this::nodeUpdated);

        cluster.join();
        client = cluster.client();
        client.cast()
    }


    private void nodeUpdated(ClusterNode node, NodeStatus status) {
        switch (status) {
            case UP:
                partitions.addRemotePartition(node.);
                Node remoteNode = new RemoteNode(node.address);
                nodes.put(remoteNode.id(), remoteNode);
                break;
            case DOWN:
                Node remoteNode = new RemoteNode(node.address);
                nodes.get(remoteNode.id()).re;
                break;
            case LOCKED:
                break;
            case UNKNOWN:
                break;
            default:
                throw new IllegalArgumentException("Invalid NodeStatus");
        }
    }

}
