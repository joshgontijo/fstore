package io.joshworks.eventry.network;

import io.joshworks.eventry.network.client.ClusterClient;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.Response;
import org.jgroups.blocks.executor.ExecutionRunner;
import org.jgroups.blocks.executor.ExecutionService;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
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
    private ExecutionService executionService;
    private ExecutionRunner executionRunner;

    private final ExecutorService consumerPool = Executors.newFixedThreadPool(10);
    private final ExecutorService taskPool = Executors.newFixedThreadPool(1);

    private final Map<Address, ClusterNode> nodesByAddress = new ConcurrentHashMap<>();
    private final Map<String, ClusterNode> nodeById = new ConcurrentHashMap<>();
    private final Map<Class, Function> handlers = new ConcurrentHashMap<>();

    private final List<BiConsumer<Message, ClusterMessage>> interceptors = new ArrayList<>();

    private static final Function<? super ClusterMessage, ClusterMessage> NO_OP = msg -> {
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
            channel = new JChannel(Thread.currentThread().getContextClassLoader().getResourceAsStream("jgroups-stack.xml"));
            channel.setDiscardOwnMessages(true);
            channel.setName(nodeUuid);

            dispatcher = new MessageDispatcher(channel, this);
            dispatcher.setMembershipListener(this);
            dispatcher.setAsynDispatching(true);

            LockService lockService = new LockService(channel);
            executionService = new ExecutionService(channel);

            executionRunner = new ExecutionRunner(channel);
            for (int i = 0; i < 1; i++) {
                taskPool.submit(executionRunner);
            }

            clusterClient = new ClusterClient(dispatcher, lockService, executionService, serializer);

            channel.connect(clusterName, null, 10000); //connect + getState

        } catch (Exception e) {
            throw new RuntimeException("Failed to join cluster", e);
        }
    }

    public ClusterClient client() {
        return clusterClient;
    }

    public synchronized void interceptor(BiConsumer<Message, ClusterMessage> interceptor) {
        interceptors.add(interceptor);
    }

    public synchronized <T extends ClusterMessage> void register(Class<T> type, Function<T, ClusterMessage> handler) {
//        KryoStoreSerializer.register(type);
        handlers.put(type, handler);
    }

    public synchronized <T extends ClusterMessage> void register(Class<T> type, Consumer<T> handler) {
//        KryoStoreSerializer.register(type);
        handlers.put(type, bb -> {
            handler.accept((T) bb);
            return null;
        });
    }

    public Address address() {
        return channel.getAddress();
    }

    public ClusterNode node(String nodeId) {
        return nodeById.get(nodeId);
    }

    public synchronized void leave() {
        IOUtils.closeQuietly(channel);
    }

    public List<ClusterNode> nodes() {
        return new ArrayList<>(nodesByAddress.values());
    }

    @Override
    public synchronized void viewAccepted(View view) {

        logger.info("View updated: {}", view);
        if (state != null) {
            for (Address address : view.getMembers()) {
                if (!state.containsMember(address)) {
                    System.out.println("[" + address() + "] Node joined: " + address);
                    addNode(address);
                }
            }
            for (Address address : state.getMembers()) {
                if (!view.containsMember(address)) {
                    System.out.println("[" + address() + "] Node left: " + address);
                    updateNodeStatus(address, NodeStatus.DOWN);
                }
            }

        } else {
            for (Address address : view.getMembers()) {
                if (!this.channel.getAddress().equals(address)) {
                    System.out.println("[" + address() + "] Already connected nodes: " + address);
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
        if (channel != null) {
            channel.disconnect();
        }
        IOUtils.closeQuietly(dispatcher);
        if (executionService != null) {
            executionService.shutdown();
        }
    }

    @Override
    public Object handle(Message msg) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(msg.buffer());
            if (!bb.hasRemaining()) {
                logger.warn("Empty message received from {}", msg.getSrc());
                intercept(msg, null);
                return null;
            }
            ClusterMessage clusterMessage = (ClusterMessage) serializer.fromBytes(ByteBuffer.wrap(msg.buffer()));

            intercept(msg, clusterMessage);

            ClusterMessage resp = (ClusterMessage) handlers.getOrDefault(clusterMessage.getClass(), NO_OP).apply(clusterMessage);
            if (resp == null) {
                //should never return null, otherwise client will block
                return null;
            }
            ByteBuffer data = serializer.toBytes(resp);
            return new Message(msg.src(), data.array()).setSrc(address());
        } catch (Exception e) {
            logger.error("Failed to receive message: " + msg, e);
            throw new RuntimeException(e);//TODO improve
        }
    }

    @Override
    public void handle(Message msg, Response response) {
        consumerPool.execute(() -> {
            try {
                //TODO improve the instantiation of the serializer
                final KryoStoreSerializer serializer = new KryoStoreSerializer();
                ByteBuffer bb = ByteBuffer.wrap(msg.buffer());
                if (!bb.hasRemaining()) {
                    logger.warn("Empty message received from {}", msg.getSrc());
                    intercept(msg, null);
                    return;
                }

                byte[] buffer = msg.buffer();
                ClusterMessage clusterMessage = (ClusterMessage) serializer.fromBytes(ByteBuffer.wrap(buffer));
                intercept(msg, clusterMessage);

                ClusterMessage resp = (ClusterMessage) handlers.getOrDefault(clusterMessage.getClass(), NO_OP).apply(clusterMessage);

                sendResponse(response, resp, msg.src());
            } catch (Exception e) {
                logger.error("Failed to receive message: " + msg, e);
                throw new RuntimeException(e);//TODO improve
            }
        });
    }

    private void sendResponse(Response response, ClusterMessage replyMessage, Address dst) throws Exception {
        if (response == null) {
            return;
        }
        replyMessage = Optional.ofNullable(replyMessage).orElse(new NullMessage());
        ByteBuffer data = serializer.toBytes(replyMessage);
        //This is required to get JGroups to work with Message
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(data.limit() + Integer.BYTES, true);
        Util.objectToStream(data.array(), out);
        Message rsp = new Message(dst, out.getBuffer());
        response.send(rsp, false);
    }

    private void intercept(Message message, ClusterMessage entity) {
        for (BiConsumer<Message, ClusterMessage> interceptor : interceptors) {
            try {
                interceptor.accept(message, entity);
            } catch (Exception e) {
                logger.warn("Failed to process interceptor '{}': {}", interceptor.getClass(), e.getMessage());
                return;
            }
        }
    }

    private void addNode(Address address) {
        PhysicalAddress physicalAddress = (PhysicalAddress) channel.down(new Event(Event.GET_PHYSICAL_ADDRESS, address));
        if (physicalAddress instanceof IpAddress) {
            IpAddress ipAddr = (IpAddress) physicalAddress;
            InetAddress inetAddr = ipAddr.getIpAddress();
            ClusterNode node = new ClusterNode(address, new InetSocketAddress(inetAddr, ipAddr.getPort()));
            nodesByAddress.put(address, node);
            nodeById.put(address.toString(), node);
        } else {
            ClusterNode node = new ClusterNode(address);
            nodesByAddress.put(address, node);
            nodeById.put(address.toString(), node);
        }

    }

    private void updateNodeStatus(Address address, NodeStatus status) {
        ClusterNode clusterNode = nodesByAddress.get(address);
        if (clusterNode == null) {
            throw new IllegalArgumentException("No such node for: " + address);
        }
        clusterNode.status = status;
    }
}
