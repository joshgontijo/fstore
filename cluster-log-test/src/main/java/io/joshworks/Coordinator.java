package io.joshworks;

import io.joshworks.fstore.cluster.Cluster;
import io.joshworks.fstore.cluster.NodeInfo;
import org.jgroups.Address;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Coordinator {

    private Address leader;
    //address - commitIndex
    private final Map<Address, AtomicLong> followers = new ConcurrentHashMap<>();

    private void onNodeConnected() {

    }


    public void electLeader(Cluster cluster, long lastSequence) {

        for (NodeInfo node : cluster.nodes()) {
            ClusterRpc proxy = cluster.rpcProxy(node.address);
            long commitIndex = proxy.getCommitIndex();
            commitIndexes.put(nodeInfo.id, new AtomicLong());
            commitIndexes.get(nodeInfo.id).set(commitIndex);
        }





    }
}
