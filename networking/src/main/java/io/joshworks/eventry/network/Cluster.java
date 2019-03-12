package io.joshworks.eventry.network;

import io.joshworks.eventry.network.client.ClusterClient;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

public class Cluster implements MembershipListener, RequestHandler, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private final String clusterName;
    private final String nodeUuid;
    private final KryoStoreSerializer serializer = new KryoStoreSerializer();

    private JChannel channel;
    private View state;
    private MessageDispatcher dispatcher;
    private ClusterClient clusterClient;

    private final ExecutorService consumerPool = Executors.newFixedThreadPool(10);

    private final Map<Address, ClusterNode> nodes = new ConcurrentHashMap<>();
    private final Map<Class, Function<NodeMessage, ClusterMessage>> handlers = new ConcurrentHashMap<>();

    private static final Function<NodeMessage, ClusterMessage> NO_OP = msg -> {
        logger.warn("No message handler for code {}", msg.getClass().getName());
        return null; //This will cause sync clients to fail
    };

    public Cluster(String clusterName, String nodeUuid) {
        this.clusterName = clusterName;
        this.nodeUuid = nodeUuid;

    }

    public synchronized void join() {
        if (channel != null) {
            throw new RuntimeException("Already joined cluster '" + clusterName + "'");
        }
        logger.info("Joining cluster '{}'", clusterName);
        try {
            //event channel
            channel = new JChannel(Thread.currentThread().getContextClassLoader().getResourceAsStream("udp.xml"));
            channel.setDiscardOwnMessages(true);
            channel.setName(nodeUuid);

            dispatcher = new MessageDispatcher(channel, this);
            dispatcher.setMembershipListener(this);
            dispatcher.setAsynDispatching(true);

            clusterClient = new ClusterClient(dispatcher, serializer);

            channel.connect(clusterName, null, 10000); //connect + getState
            addNode(channel.address());

        } catch (Exception e) {
            throw new RuntimeException("Failed to join cluster", e);
        }
    }

    public ClusterClient client() {
        return clusterClient;
    }

    public synchronized void register(Class<? extends ClusterMessage> type, Function<NodeMessage, ClusterMessage> handler) {
        serializer.register(type);
        handlers.put(type, handler);
    }

    public synchronized void register(Class<? extends ClusterMessage> type, Consumer<NodeMessage> handler) {
        serializer.register(type);
        handlers.put(type, bb -> {
            handler.accept(bb);
            return null;
        });
    }

    public Address address() {
        return channel.getAddress();
    }

    public synchronized void leave() {
        updateNodeStatus(channel.address(), NodeStatus.DOWN);
        channel.close();
    }

    public List<ClusterNode> nodes() {
        return new ArrayList<>(nodes.values());
    }

    @Override
    public synchronized void viewAccepted(View view) {
        logger.info("View updated: {}", view);
        if (state != null) {
            for (Address address : view.getMembers()) {
                if (!state.containsMember(address)) {
                    System.out.println("Node joined: " + address);
                    addNode(address);
                }
            }
            for (Address address : state.getMembers()) {
                if (!view.containsMember(address)) {
                    System.out.println("Node left: " + address);
                    updateNodeStatus(address, NodeStatus.DOWN);
                }
            }

        } else {
            for (Address address : view.getMembers()) {
                if (!this.channel.getAddress().equals(address)) {
                    System.out.println("Already connected nodes: " + address);
                    addNode(address);
                }
            }
        }
        state = view;
    }

    @Override
    public void suspect(Address mbr) {
        logger.warn("SUSPECT ADDRESS: {}", mbr);
    }

    @Override
    public void close() {
        channel.disconnect();
        IOUtils.closeQuietly(dispatcher);
    }

    @Override
    public Object handle(Message msg) {
        try {
            ClusterMessage clusterMessage = (ClusterMessage) serializer.fromBytes(ByteBuffer.wrap(msg.buffer()));
            NodeMessage nodeMessage = new NodeMessage(msg.src(), clusterMessage, msg.src(), response, serializer);
            ClusterMessage response = handlers.getOrDefault(clusterMessage.getClass(), NO_OP).apply(nodeMessage);
            if (response == null) {
                return null; //TODO will null actually send a response message ?
            }
            ByteBuffer data = serializer.toBytes(response);
            return new Message(msg.src(), data.array()).setSrc(address());
        } catch (Exception e) {
            logger.error("Failed to receive message: " + msg, e);
            throw new RuntimeException(e);//TODO improve
        }
    }

    @Override
    public void handle(Message request, Response response) throws Exception {
        MessageResponse resp = new MessageResponse(request.src(), address(), response, serializer);
        consumerPool.execute(() ->{
            handle(request);
        });

        System.err.println("##########################");
    }

    private void addNode(Address address) {
        nodes.put(address, new ClusterNode(address));
    }

    private void updateNodeStatus(Address address, NodeStatus status) {
        ClusterNode clusterNode = nodes.get(address);
        if (clusterNode == null) {
            throw new IllegalArgumentException("No such node for: " + address);
        }
        clusterNode.status = status;
    }
}
