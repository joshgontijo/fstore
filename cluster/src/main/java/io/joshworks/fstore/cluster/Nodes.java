package io.joshworks.fstore.cluster;

import org.jgroups.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Nodes {

    private final Map<Address, ClusterNode> nodesByAddress = new ConcurrentHashMap<>();
    private final Map<String, ClusterNode> nodeById = new ConcurrentHashMap<>();


    public void add(Address address, ClusterNode node) {
        nodesByAddress.put(address, node);
        nodeById.put(address.toString(), node);
    }

    public ClusterNode byAddress(Address address) {
        return nodesByAddress.get(address);
    }

    public ClusterNode byId(String id) {
        return nodeById.get(id);
    }

    public List<ClusterNode> all() {
        return new ArrayList<>(nodesByAddress.values());
    }
}
