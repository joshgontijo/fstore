package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.NodeStatus;
import org.jgroups.Address;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Nodes {

    private final Map<String, ClusterNode> byUuid = new ConcurrentHashMap<>();
    private final Map<Address, ClusterNode> byAddress = new ConcurrentHashMap<>();

    public synchronized void add(ClusterNode node) {
        byUuid.put(node.uuid, node);
        byAddress.put(node.address, node);
    }

    public synchronized void updateStatus(String uuid, NodeStatus status) {
        ClusterNode clusterNode = byUuid.get(uuid);
        if(clusterNode == null) {
            throw new IllegalArgumentException("No such node for UUID: " + uuid);
        }
        clusterNode.status = status;
    }

    public Address fromUuid(String uuid) {
        ClusterNode node = byUuid.get(uuid);
        return node == null ? null : node.address;
    }

    public String fromAddress(Address address) {
        ClusterNode node = byAddress.get(address);
        return node == null ? null : node.uuid;
    }

}
