package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.eventry.server.cluster.data.NodeInfo;
import io.joshworks.eventry.server.cluster.message.NodeInfoRequested;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Cluster extends ReceiverAdapter {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private static final Serializer<EventRecord> serializer = new EventSerializer();
    private static final Serializer<NodeInfo> nodeInfoSerializer = JsonSerializer.of(NodeInfo.class);

    private JChannel channel;
    private final String clusterName;
    private final String nodeUuid;
    private View state;
    private MessageDispatcher dispatcher;

    private final Map<String, Address> nodes = new ConcurrentHashMap<>();
    private final EventHandler eventHandler;

    public Cluster(String clusterName, String nodeUuid) {
        this.clusterName = clusterName;
        this.nodeUuid = nodeUuid;
        this.eventHandler = new EventHandler(this);
    }

    public synchronized void join() {
        if (channel != null) {
            throw new RuntimeException("Already joined");
        }
        logger.info("Joining cluster");
        try {
            channel = new JChannel(Thread.currentThread().getContextClassLoader().getResourceAsStream("tcp.xml"));
            channel.setReceiver(this);
            channel.setDiscardOwnMessages(false);
            channel.setName(nodeUuid);
            channel.connect(clusterName);
            channel.getState(null, 10000);
            dispatcher = new MessageDispatcher(channel);
            dispatcher.setRequestHandler(eventHandler);
            new Thread(() -> {
                try {
                    while(true) {
                    Thread.sleep(5000);
                    System.out.println(state);

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to join cluster", e);
        }
    }

    public void register(String eventType, Consumer<ClusterMessage> consumer) {
        this.eventHandler.register(eventType, consumer);
    }

    public Address address() {
        return channel.getAddress();
    }

    public synchronized void leave() {
        channel.close();
    }

    public List<Address> members() {
        return new ArrayList<>(state.getMembers());
    }

    public List<Address> otherNodes() {
        return state.getMembers().stream().filter(address -> !address.equals(address())).collect(Collectors.toList());
    }


    private RspList<byte[]> castSyncInternal(Buffer msg) {
        try {
            return dispatcher.castMessage(null, msg, RequestOptions.SYNC());
        } catch (Exception e) {
            throw new ClusterException(e);
        }
    }


    public List<NodeInfo> getClusterInfo() {
        RspList<byte[]> rsps = castSyncInternal(createBuffer(NodeInfoRequested.create(nodeUuid)));
        List<NodeInfo> results = new ArrayList<>();
        for (Rsp<byte[]> rsp : rsps) {
            byte[] data = rsp.getValue();
            NodeInfo nodeInfo = nodeInfoSerializer.fromBytes(ByteBuffer.wrap(data));
            results.add(nodeInfo);
        }
        return results;
    }

    public void transferPartition() {

    }

    /**
     * Sends a message and waits for the response
     */
    public EventRecord send(Address address, EventRecord event) {
        try {
            Message response = dispatcher.sendMessage(address, createBuffer(event), RequestOptions.SYNC());
            if (response == null) {
                return null;
            }
            return serializer.fromBytes(ByteBuffer.wrap(response.buffer()));
        } catch (Exception e) {
            throw new ClusterException(e);
        }
    }

    /**
     * Sends a message and synchronously process the response on the EVENT HANDLER
     */
    public void sendTo(Address address, EventRecord event) {
        try {
            Message response = dispatcher.sendMessage(address, createBuffer(event), RequestOptions.SYNC());
            eventHandler.handle(response);
        } catch (Exception e) {
            throw new ClusterException(e);
        }
    }

    /**
     * Sends a message and asynchronously process the response on the EVENT HANDLER
     */
    public void asyncSend(Address address, EventRecord event) {
        try {
            CompletableFuture<Message> future = dispatcher.sendMessageWithFuture(address, createBuffer(event), RequestOptions.ASYNC());
            future.thenAccept(eventHandler::handle);
        } catch (Exception e) {
            throw new ClusterException(e);
        }
    }

    public List<ClusterMessage> syncCast(EventRecord event) {
        try {
            RspList<Message> rsps = dispatcher.castMessage(null, createBuffer(event), RequestOptions.SYNC());
            List<ClusterMessage> replies = new ArrayList<>();
            for (Rsp<Message> rsp : rsps) {
                Message value = rsp.getValue();
                replies.add(ClusterMessage.from(value));
            }
            return replies;
        } catch (Exception e) {
            throw new ClusterException(e);
        }
    }

    private Buffer createBuffer(EventRecord event) {
        return new Buffer(serializer.toBytes(event).array());
    }

    private Address addressOf(String uuid) {
        Address address = nodes.get(uuid);
        if (address == null) {
            throw new IllegalArgumentException("Node not found: " + uuid);
        }
        return address;
    }

    @Override
    public void viewAccepted(View view) {
        logger.info("View updated: {}", view);
        if (state != null) {
            for (Address address : view.getMembers()) {
                if (!state.containsMember(address)) {
                    System.out.println("Node joined: " + address);
                }
            }
            for (Address address : state.getMembers()) {
                if (!view.containsMember(address)) {
                    System.out.println("Node left: " + address);
                }
            }

        } else {
            for (Address address : view.getMembers()) {
                if (!this.channel.getAddress().equals(address)) {
                    System.out.println("Already connected nodes: " + address);
                }
//                    eventBus.emit(new NodeJoined(inetAddress(address)));
            }
        }
        state = view;
    }

    @Override
    public void receive(Message msg) {
        super.receive(msg);
    }

    @Override
    public void suspect(Address mbr) {
        logger.warn("SUSPECT ADDRESS: {}", mbr);
    }
}
