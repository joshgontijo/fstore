package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.fstore.core.Serializer;
import org.jgroups.Message;
import org.jgroups.blocks.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class EventHandler implements RequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(EventHandler.class);
    private static final Serializer<EventRecord> serializer = new EventSerializer();
    private final Cluster cluster;

    private final Map<String, Consumer<ClusterMessage>> handlers = new ConcurrentHashMap<>();

    private static final Consumer<ClusterMessage> NO_OP = cm -> logger.warn("No message handler for {}", cm.message());

    public EventHandler(Cluster cluster) {
//        ClassConfigurator.add(EventHeader.HEADER_ID, EventHeader.class);
        this.cluster = cluster;
    }

    @Override
    public Object handle(Message msg) {
        return handleEvent(msg);
    }

    public void register(String eventType, Consumer<ClusterMessage> consumer) {
        this.handlers.put(eventType, consumer);
    }

    private Message handleEvent(Message msg) {
        try {
            System.out.println("[EVENT-HANDLER] " + msg);

            ClusterMessage cMessage = ClusterMessage.from(msg);
            if (cMessage.message() == null) {
                logger.warn("No event data received from {}", msg.src());
                return null; //ERROR ? sender did not send anything
            }
            handlers.getOrDefault(cMessage.message().type, NO_OP).accept(cMessage);
            if (cMessage.reply == null) {
                return null;
            }
            byte[] replyData = serializer.toBytes(cMessage.reply).array();
            return new Message(cMessage.sender(), replyData).setSrc(cluster.address());


        } catch (Exception e) {
            logger.error("Failed to receive message: " + msg, e);
        }
        return null;

    }
}
