package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.cluster.client.AddressMapper;
import io.joshworks.eventry.server.cluster.client.ClusterClient;
import io.joshworks.eventry.server.cluster.client.NodeMessage;
import io.joshworks.fstore.core.io.IOUtils;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Cluster implements MembershipListener, RequestHandler, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private final String clusterName;
    private final String nodeUuid;

    private JChannel channel;
    private View state;
    private MessageDispatcher dispatcher;
    private ClusterClient clusterClient;

    private final Nodes nodes = new Nodes();

    private final Map<Integer, Function<NodeMessage, NodeMessage>> handlers = new ConcurrentHashMap<>();

    private static final Function<NodeMessage, NodeMessage> NO_OP = bb -> {
        logger.warn("No message handler for code {}", bb.code);
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
            channel = new JChannel(Thread.currentThread().getContextClassLoader().getResourceAsStream("tcp.xml"));
            channel.setDiscardOwnMessages(true);
            channel.setName(nodeUuid);

            dispatcher = new MessageDispatcher(channel, this);
            dispatcher.setMembershipListener(this);

            clusterClient = new ClusterClient(dispatcher, new AddressMapper(nodes::fromUuid, nodes::fromAddress));

            channel.connect(clusterName);
            channel.getState(null, 10000);

        } catch (Exception e) {
            throw new RuntimeException("Failed to join cluster", e);
        }
    }

    public ClusterClient client() {
        return clusterClient;
    }

    public void register(int code, Function<NodeMessage, NodeMessage> handler) {
        this.handlers.put(code, handler);
    }

    public void register(int code, Consumer<NodeMessage> handler) {
        this.handlers.put(code, bb -> {
            handler.accept(bb);
            return null;
        });
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
            ByteBuffer bb = ByteBuffer.wrap(msg.buffer());
            String srcId = nodes.fromAddress(msg.src());
            NodeMessage nodeMessage = new NodeMessage(srcId, bb);
            NodeMessage response = handlers.getOrDefault(nodeMessage.code, NO_OP).apply(nodeMessage);
            if (response == null) {
                return null; //TODO will null actually send a response message ?
            }
            byte[] replyData = response.buffer.array();
            return new Message(msg.src(), replyData).setSrc(address());
        } catch (Exception e) {
            logger.error("Failed to receive message: " + msg, e);
            throw new RuntimeException(e);//TODO improve
        }
    }
}
