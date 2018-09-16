package io.joshworks.eventry.server;

import io.joshworks.eventry.server.cluster.TcpClient;
import io.joshworks.eventry.server.cluster.WelcomeMessage;
import io.joshworks.eventry.server.cluster.data.NodeInfo;
import io.joshworks.fstore.core.eventbus.Subscribe;

import java.net.InetSocketAddress;

public class NodeListener {

    @Subscribe
    public void onNodeJoined(NodeInfo info) {
        TcpClient client = new TcpClient(new InetSocketAddress(info.host, info.port));
        client.connect();
        Object response = client.send(new WelcomeMessage("Welcome !"));
        System.out.println("--> " + response);
    }

}