package io.joshworks.fstore.cluster;

import io.joshworks.fstore.cluster.rpc.RpcClient;
import io.joshworks.fstore.cluster.rpc.RpcMessage;
import io.joshworks.fstore.cluster.rpc.RpcReceiver;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.serializer.kryo.KryoSerializer;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class Cluster implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);
    private static final BiFunction<Address, Object, Object> NO_OP = (addr, msg) -> {
        logger.warn("No message handler for code {}", msg.getClass().getName());
        return null;
    };
    private final AtomicBoolean closed = new AtomicBoolean();
    private final String clusterName;
    private final String nodeId;
    private final boolean discardOwnMessages;
    private final ExecutorService consumerPool = Executors.newFixedThreadPool(10);
    private final ExecutorService taskPool = Executors.newFixedThreadPool(1);
    private final ExecutorService statePool = Executors.newSingleThreadExecutor();
    private final Nodes nodes = new Nodes();
    private final Map<Class<?>, BiFunction<Address, Object, Object>> handlers = new ConcurrentHashMap<>();
    private final Map<Address, Object> rpcProxyClients = new ConcurrentHashMap<>();
    private final List<Consumer<NodeInfo>> nodeUpdatedListeners = new ArrayList<>();
    private final List<BiConsumer<Address, Object>> interceptors = new ArrayList<>();
    private final List<Runnable> connectionListeners = new ArrayList<>();
    private JChannel channel;
    private View state;
    private MessageDispatcher dispatcher;
    private ClusterClient client;
    private RpcClient rpcClient;
    private Class<?> rpcProxyType;
    private ExecutionService executionService;
    private ExecutionRunner executionRunner;

    public Cluster(String clusterName, String nodeId) {
        this(clusterName, nodeId, true);
    }

    public Cluster(String clusterName, String nodeId, boolean discardOwnMessages) {
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.discardOwnMessages = discardOwnMessages;
    }

    public synchronized void join() {
        if (channel != null) {
            throw new RuntimeException("Already joined cluster '" + clusterName + "'");
        }
        if (closed.get()) {
            throw new IllegalStateException("Already left the cluster");
        }
        logger.info("Joining cluster '{}'", clusterName);
        try {
            //event channel
            channel = new JChannel(Thread.currentThread().getContextClassLoader().getResourceAsStream("jgroups-stack.xml"));
            channel.setDiscardOwnMessages(discardOwnMessages);
            channel.setName(nodeId);

            EventReceiver receiver = new EventReceiver();

            dispatcher = new MessageDispatcher(channel, receiver);
            dispatcher.setMembershipListener(receiver);
            dispatcher.setAsynDispatching(true);

            LockService lockService = new LockService(channel);
            executionService = new ExecutionService(channel);

            executionRunner = new ExecutionRunner(channel);
            taskPool.submit(executionRunner);

            client = new ClusterClient(dispatcher, lockService, executionService);
            rpcClient = new RpcClient(client);

            channel.connect(clusterName, null, 10000); //connect + getState

            connectionListeners.forEach(Runnable::run);

        } catch (Exception e) {
            throw new RuntimeException("Failed to join cluster", e);
        }
    }

    public ClusterClient client() {
        return client;
    }

    public RpcClient rpcClient() {
        return rpcClient;
    }

    public <T> T rpcProxy(Address address) {
        Object proxy = rpcProxyClients.get(address);
        return (T) proxy;
    }

    public synchronized void interceptor(BiConsumer<Address, Object> interceptor) {
        interceptors.add(interceptor);
    }

    public synchronized void onConnected(Runnable runnable) {
        connectionListeners.add(runnable);
    }

    public synchronized void onNodeUpdated(Consumer<NodeInfo> handler) {
        nodeUpdatedListeners.add(handler);
    }

    public synchronized void registerRpcHandler(Object handler) {
        handlers.put(RpcMessage.class, RpcReceiver.create(handler));
    }

    public synchronized <T> void register(Class<T> type, Function<T, Object> handler) {
        handlers.put(type, (address, bb) -> handler.apply((T) bb));
    }

    public synchronized <T> void register(Class<T> type, BiFunction<Address, T, Object> handler) {
        handlers.put(type, (BiFunction<Address, Object, Object>) handler);
    }

    public synchronized <T> void register(Class<T> type, BiConsumer<Address, T> handler) {
        handlers.put(type, (address, bb) -> {
            handler.accept(address, (T) bb);
            return null;
        });
    }

    public void registerRpcProxy(Class<?> proxyType) {
        this.rpcProxyType = proxyType;
    }

    private void fireNodeUpdate(NodeInfo node) {
        for (Consumer<NodeInfo> listener : nodeUpdatedListeners) {
            listener.accept(node);
        }
    }

    public Address address() {
        return channel.getAddress();
    }

    public Address coordinator() {
        return state.getCoord();
    }

    public boolean isCoordinator() {
        return address().equals(coordinator());
    }

    public String nodeId() {
        return nodeId;
    }

    public NodeInfo node(String nodeId) {
        return nodes.byId(nodeId);
    }

    public NodeInfo node() {
        NodeInfo node = node(nodeId);
        if (node == null) {
            throw new IllegalStateException("Could not find current node, not connected to cluster");
        }
        return node;
    }

    public List<NodeInfo> nodes() {
        return nodes.all();
    }

    public int numNodes() {
        return nodes.size();
    }

    public void lock(String name, Runnable runnable) {
        Lock lock = client.lock(name);
        logger.info("Acquiring lock [{}]", name);
        lock.lock();
        logger.info("Lock acquired [{}]", name);
        try {
            runnable.run();
        } finally {
            lock.unlock();
            logger.info("Lock released [{}]", name);
        }
    }


    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (channel != null) {
            channel.disconnect();
            channel = null;
        }
        IOUtils.closeQuietly(dispatcher);
        if (executionService != null) {
            executionService.shutdown();
        }
    }


    private void intercept(Address message, Object entity) {
        for (BiConsumer<Address, Object> interceptor : interceptors) {
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
        NodeInfo node;
        if (physicalAddress instanceof IpAddress) {
            IpAddress ipAddr = (IpAddress) physicalAddress;
            InetAddress inetAddr = ipAddr.getIpAddress();
            node = new NodeInfo(address, new InetSocketAddress(inetAddr, ipAddr.getPort()));

        } else {
            node = new NodeInfo(address);
        }
        nodes.add(address, node);
        if (rpcProxyType != null) {
            rpcProxyClients.put(address, rpcClient.createProxy(rpcProxyType, address, 60000)); //TODO expose
        }
        fireNodeUpdate(node);
    }

    private void updateNodeStatus(Address address, NodeStatus status) {
        NodeInfo nodeInfo = nodes.byAddress(address);
        if (nodeInfo == null) {
            throw new IllegalArgumentException("No such node for: " + address);
        }
        nodeInfo.status = status;
        fireNodeUpdate(nodeInfo);
    }

    public String name() {
        return clusterName;
    }

    private class EventReceiver implements MembershipListener, RequestHandler {

        @Override
        public void viewAccepted(View view) {
            statePool.execute(() -> {
                logger.info("View updated: {}", view);
                View old = state;
                state = view;

                if (old != null) {
                    for (Address address : view.getMembers()) {
                        if (!old.containsMember(address)) {
                            System.out.println("[" + address() + "] Node joined: " + address);
                            addNode(address);
                        }
                    }
                    for (Address address : old.getMembers()) {
                        if (!view.containsMember(address)) {
                            System.out.println("[" + address() + "] Node left: " + address);
                            updateNodeStatus(address, NodeStatus.DOWN);
                        }
                    }

                } else {
                    for (Address address : view.getMembers()) {
                        addNode(address);
                        if (!channel.getAddress().equals(address)) {
                            System.out.println("[" + address() + "] Already connected node: " + address);
                        } else {
                            System.out.println("[" + address() + "] Current node view updated");
                        }
                    }
                }
            });

        }

        @Override
        public void suspect(Address mbr) {
            logger.warn("SUSPECT ADDRESS: {}", mbr);
        }

        @Override
        public Object handle(Message msg) {
            try {
                Object resp = handleEvent(msg);
                if (resp == null) {
                    //should never return null, otherwise client will block
                    logger.warn("NULL RESPONSE FROM HANDLER");
                    resp = new NullMessage();
                }
                byte[] data = KryoSerializer.serialize(resp);
                return new Message(msg.src(), data).setSrc(address());
            } catch (Exception e) {
                logger.error("Failed to receive message: " + msg, e);
                return new ErrorMessage(e.getMessage());
            }
        }

        @Override
        public void handle(Message msg, Response response) {
            consumerPool.execute(() -> {
                try {
                    Object resp = handleEvent(msg);
                    sendResponse(response, resp, msg.src());
                } catch (Exception e) {
                    logger.error("Failed handling message: " + msg, e);
                    try {
                        sendResponse(response, new ErrorMessage(e.getMessage()), msg.src());
                    } catch (Exception ex) {
                        logger.error("Failed sending error message back to caller", e);
                    }
                }
            });
        }

        private Object handleEvent(Message msg) {
            ByteBuffer bb = ByteBuffer.wrap(msg.buffer());
            if (!bb.hasRemaining()) {
                logger.warn("Empty message received from {}", msg.getSrc());
                intercept(msg.src(), null);
                return null;
            }

            Object clusterMessage = KryoSerializer.deserialize(msg.buffer());
            intercept(msg.src(), clusterMessage);

            return handlers.getOrDefault(clusterMessage.getClass(), NO_OP).apply(msg.src(), clusterMessage);
        }

        private void sendResponse(Response response, Object replyMessage, Address dst) throws Exception {
            if (response == null) {
                return;
            }
            replyMessage = Optional.ofNullable(replyMessage).orElse(new NullMessage());
            byte[] data = KryoSerializer.serialize(replyMessage);
            //This is required to get JGroups to work with Message
            ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(data.length + Integer.BYTES, true);
            Util.objectToStream(data, out);
            Message rsp = new Message(dst, out.getBuffer());
            response.send(rsp, false);
        }

    }

}
