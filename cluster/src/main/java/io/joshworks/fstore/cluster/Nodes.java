package io.joshworks.fstore.cluster;

import org.jgroups.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Nodes {

    private final Map<Address, NodeInfo> nodesByAddress = new ConcurrentHashMap<>();
    private final Map<String, NodeInfo> nodeById = new ConcurrentHashMap<>();


    public void add(Address address, NodeInfo node) {
        nodesByAddress.put(address, node);
        nodeById.put(address.toString(), node);
    }

    public NodeInfo byAddress(Address address) {
        return nodesByAddress.get(address);
    }

    public NodeInfo byId(String id) {
        return nodeById.get(id);
    }

    public List<NodeInfo> all() {
        return new ArrayList<>(nodesByAddress.values());
    }
}
