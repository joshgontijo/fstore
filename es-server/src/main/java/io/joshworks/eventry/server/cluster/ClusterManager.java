package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.cluster.data.NodeInfo;
import io.joshworks.eventry.server.cluster.data.NodeLeft;
import io.joshworks.fstore.core.eventbus.EventBus;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.stack.IpAddress;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class ClusterManager {

    private static final String CLUSTER_NAME = "default-cluster";

    private final EventBus eventBus;
    private final JChannel channel;
    private final String nodeId;
    private Address thisAddress;

    private View state;
    private int port;


    private final Map<Address, Node> nodes = new ConcurrentHashMap<>();


    public static void main(String[] args) throws Exception {
        EventBus bus = new EventBus();

        bus.on(String.class, (EventBus.EventConsumer<String>) System.out::println);
        int port = ThreadLocalRandom.current().nextInt(0,10);

        ClusterManager clusterManager = new ClusterManager(bus, port);
        clusterManager.connect();

//        clusterManager.send("Ola");
    }


    public ClusterManager(EventBus eventBus, int port) throws Exception {
        this.port = port;
        this.nodeId = UUID.randomUUID().toString().substring(0, 8);
        this.eventBus = eventBus;
        this.channel = new JChannel();
        this.channel.setDiscardOwnMessages(true);
        this.channel.setReceiver(new EventReceiver());


//        this.channel.name(nodeId);
//        this.channel.addChannelListener(new ChannelHandler());
//        this.dispatcher = new MessageDispatcher(channel);

    }

    public void connect() throws Exception {
        channel.connect(CLUSTER_NAME);
        System.out.println("Connected to " + CLUSTER_NAME);
        this.thisAddress = channel.getAddress();
        InetSocketAddress inetSocketAddress = inetAddress(channel.getAddress());
        String hostAddress = inetSocketAddress.getAddress().getHostAddress();
        System.out.println("Sending joined message...");
        channel.send(new Message(null, new NodeInfo(nodeId, "joined", hostAddress, port).toJson()));
    }

    public void send(String message) throws Exception {
        channel.send(null, message);
    }

    public void close() {
        channel.disconnect();
        channel.close();
    }

    private InetSocketAddress inetAddress(Address address) {
        IpAddress ipAddress = (IpAddress) channel.down(new Event(Event.GET_PHYSICAL_ADDRESS, address));
        return new InetSocketAddress(ipAddress.getIpAddress(), ipAddress.getPort());
    }

    private class EventReceiver extends ReceiverAdapter {

        @Override
        public void viewAccepted(View view) {
            if (state != null) {
                for (Address address : view.getMembers()) {
                    if (!state.containsMember(address)) {
//                        eventBus.emit(new NodeJoined(inetAddress(address)));
                        System.out.println("Node joined, standby for info...");
                    }
                }
                for (Address address : state.getMembers()) {
                    if (!view.containsMember(address)) {
                        System.out.println("Node left: " + address);
                        Node remove = nodes.remove(address);
                        eventBus.emit(new NodeLeft(remove.id, remove.address.getAddress().getHostAddress(), remove.address.getPort()));

                    }
                }

            } else {
                for (Address address : view.getMembers()) {

//                    eventBus.emit(new NodeJoined(inetAddress(address)));
                }
            }
            state = view;
        }

        @Override
        public void receive(Message msg) {
            System.out.println("Message received: " + msg);
            String json = msg.getObject();

            NodeInfo nodeInfo = NodeInfo.fromJson(json);
            if("joined".equals(nodeInfo.type)) {
                nodes.put(msg.getSrc(), new Node(nodeInfo.id, nodeInfo.host, nodeInfo.port));
                //Send this node info to the new Node
                try {
                    System.out.println("Sending this node info back to the joined node...");
                    channel.send(msg.getSrc(), new NodeInfo(nodeId, "existingNodeInfo", inetAddress(thisAddress).getHostName(), port).toJson());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if("existingNodeInfo".equals(nodeInfo.type)) {
                System.out.println("Received info from already connected node " + nodeInfo.id);
                nodes.put(msg.getSrc(), new Node(nodeInfo.id, nodeInfo.host, nodeInfo.port));
            }


        }

    }

}
