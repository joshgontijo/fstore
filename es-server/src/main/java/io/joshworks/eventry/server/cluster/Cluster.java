package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.eventry.server.cluster.message.NodeInfoReceived;
import io.joshworks.eventry.server.cluster.message.NodeJoined;
import io.joshworks.eventry.server.cluster.message.NodeLeft;
import io.joshworks.eventry.server.cluster.message.PartitionCreated;
import io.joshworks.eventry.server.cluster.message.PartitionForked;
import io.joshworks.eventry.server.cluster.message.PartitionTransferred;
import io.joshworks.eventry.server.cluster.message.command.ClusterCommand;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.eventbus.EventBus;
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
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Cluster extends ReceiverAdapter implements RequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private static final Serializer<EventRecord> serializer = new EventSerializer();

    private JChannel channel;
    private final String clusterName;
    private final String nodeUuid;
    private View state;
    private MessageDispatcher dispatcher;
    private final EventBus eventBus;

    private final Map<String, Address> nodes = new ConcurrentHashMap<>();

    public Cluster(String clusterName, String nodeUuid, EventBus eventBus) {
        this.clusterName = clusterName;
        this.nodeUuid = nodeUuid;
        this.eventBus = eventBus;
    }

    public synchronized void join() {
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

    public RspList<Message> cast(EventRecord eventRecord) {
        try {
            return dispatcher.castMessage(null, createEvent(cmd), RequestOptions.ASYNC());
        } catch (Exception e) {
            throw new ClusterException(e);
        }
    }

    public Object sendTo(String uuid, ClusterCommand cmd) {
        try {
            return dispatcher.sendMessage(node(uuid), createEvent(cmd), RequestOptions.ASYNC());
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

    private Buffer createEvent(ClusterCommand command) {
        EventRecord event = command.toEvent(channel.getAddress());
        return new Buffer(serializer.toBytes(event).array());
    }

    private Address node(String uuid) {
        Address address = nodes.get(uuid);
        if(address == null) {
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
    public Object handle(Message msg) {
        handleEvent(msg);
        return null;
    }

    @Override
    public void handle(Message msg, Response response) {
        handleEvent(msg);
    }

    private void handleEvent(Message msg) {
        try {
            EventRecord record = serializer.fromBytes(ByteBuffer.wrap(msg.buffer()));
            switch (record.type) {
                case NodeJoined.TYPE:
                    NodeJoined joined = NodeJoined.from(record);
                    nodes.put(joined.uuid, msg.getSrc());
                    eventBus.emit(joined);
                    break;
                case NodeLeft.TYPE:
                    NodeLeft left = NodeLeft.from(record);
                    nodes.remove(left.uuid);
                    eventBus.emit(left);
                    break;
                case NodeInfoReceived.TYPE:
                    eventBus.emit(NodeInfoReceived.from(record));
                    break;
                case PartitionCreated.TYPE:
                    eventBus.emit(PartitionCreated.from(record));
                    break;
                case PartitionTransferred.TYPE:
                    eventBus.emit(PartitionTransferred.from(record));
                    break;
                case PartitionForked.TYPE:
                    eventBus.emit(PartitionForked.from(record));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown cluster message: " + record);
            }
        } catch (Exception e) {
            logger.error("Failed to receive message: " + msg, e);
        }

    }

}
