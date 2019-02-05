package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.eventry.server.ClusterEvents;
import io.joshworks.eventry.server.cluster.data.NodeInfo;
import io.joshworks.eventry.server.cluster.message.NodeInfoRequested;
import io.joshworks.eventry.server.cluster.message.NodeJoined;
import io.joshworks.eventry.server.cluster.message.NodeLeft;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.Response;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class Cluster extends ReceiverAdapter implements RequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private static final Serializer<EventRecord> serializer = new EventSerializer();
    private static final Serializer<NodeInfo> nodeInfoSerializer = JsonSerializer.of(NodeInfo.class);

    private JChannel channel;
    private final String clusterName;
    private final String nodeUuid;
    private View state;
    private MessageDispatcher dispatcher;

    private final Map<String, Address> nodes = new ConcurrentHashMap<>();
    private ClusterEvents clusterEvents;

    public Cluster(String clusterName, String nodeUuid) {
        this.clusterName = clusterName;
        this.nodeUuid = nodeUuid;
    }

    public synchronized void join(ClusterEvents clusterEvents) {
        this.clusterEvents = requireNonNull(clusterEvents, "EventHandler must be provided");
        if (channel != null) {
            throw new RuntimeException("Already joined");
        }
        logger.info("Joining cluster");
        try {
            channel = new JChannel();
            channel.setReceiver(this);
            channel.setDiscardOwnMessages(true);
            channel.setName(nodeUuid);
            channel.connect(clusterName);
            channel.getState(null, 10000);
            dispatcher = new MessageDispatcher(channel);
            dispatcher.setRequestHandler(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to join cluster", e);
        }
    }

    public synchronized void leave() {
        channel.close();
    }

    public List<Address> members() {
        return new ArrayList<>(state.getMembers());
    }

    public List<Address> otherNodes() {
        return state.getMembers().stream().filter(a -> channel.getAddress() != a).collect(Collectors.toList());
    }

    public void cast(EventRecord event) {
        try {
            dispatcher.castMessage(null, createBuffer(event), RequestOptions.ASYNC());
        } catch (Exception e) {
            throw new ClusterException(e);
        }
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

    public void transfer() {

    }

    public Object sendTo(String uuid, EventRecord event) {
        try {
            return dispatcher.sendMessage(node(uuid), createBuffer(event), RequestOptions.ASYNC());
        } catch (Exception e) {
            throw new ClusterException(e);
        }
    }

//    public void sendAsyncTo(Address address, ClusterCommand cmd) {
//        try {
//            dispatcher.sendMessage(address, createEvent(cmd), RequestOptions.ASYNC());
//        } catch (Exception e) {
//            throw new ClusterException(e);
//        }
//    }

    private Buffer createBuffer(EventRecord event) {
        return new Buffer(serializer.toBytes(event).array());
    }

    private Address node(String uuid) {
        Address address = nodes.get(uuid);
        if (address == null) {
            throw new IllegalArgumentException("Node not found: " + uuid);
        }
        return address;
    }

    @Override
    public void viewAccepted(View view) {
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
    public Object handle(Message msg) {
        return handleEvent(msg);
    }

    @Override
    public void handle(Message msg, Response response) {
        handleEvent(msg);
    }

    private byte[] handleEvent(Message msg) {
        try {
            EventRecord record = serializer.fromBytes(ByteBuffer.wrap(msg.buffer()));
            switch (record.type) {
                case NodeJoined.TYPE:
                    NodeJoined joined = NodeJoined.from(record);
                    nodes.put(joined.uuid, msg.getSrc());
                    clusterEvents.onNodeJoined(joined);
                    break;
                case NodeLeft.TYPE:
                    NodeLeft left = NodeLeft.from(record);
                    nodes.remove(left.uuid);
                    clusterEvents.onNodeLeft(left);
                    break;
                case NodeInfoRequested.TYPE:
                    NodeInfo nodeInfo = clusterEvents.onNodeInfoRequested();
                    return nodeInfoSerializer.toBytes(nodeInfo).array();
                default:
                    throw new IllegalArgumentException("Unknown cluster message: " + record);
            }
        } catch (Exception e) {
            logger.error("Failed to receive message: " + msg, e);
        }
        return null;

    }

}
