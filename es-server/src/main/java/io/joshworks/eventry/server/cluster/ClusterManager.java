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
import java.util.UUID;

public class ClusterManager {

    private static final String CLUSTER_NAME = "default-cluster";

    private final EventBus eventBus;
    private final JChannel channel;
    private final String nodeId;

    private View state;
    private int port;

    //sync message (request / response)
//    MessageDispatcher dispatcher;
    public static void main(String[] args) throws Exception {
        EventBus bus = new EventBus();

        bus.on(String.class, (EventBus.EventConsumer<String>) System.out::println);

        ClusterManager clusterManager = new ClusterManager(bus, 5555);
        clusterManager.connect();

        clusterManager.send("Ola");
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
        InetSocketAddress inetSocketAddress = inetAddress(channel.getAddress());
        String hostAddress = inetSocketAddress.getAddress().getHostAddress();
        channel.send(new Message(null, new NodeInfo("joined", hostAddress, port).toJson()));
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
                    }
                }
                for (Address address : state.getMembers()) {
                    if (!view.containsMember(address)) {
                        eventBus.emit(new NodeLeft(inetAddress(address)));
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
            eventBus.emit(NodeInfo.fromJson(json));
        }

    }

//    private class ChannelHandler implements ChannelListener {
//
//        @Override
//        public void channelConnected(JChannel channel) {
//            System.out.println("Channel connected: " + channel.name());
//        }
//
//        @Override
//        public void channelDisconnected(JChannel channel) {
//            System.out.println("Channel disconnected: " + channel.name());
//        }
//
//        @Override
//        public void channelClosed(JChannel channel) {
//            System.out.println("Channel closed: " + channel.name());
//        }
//
//
//    }


}
