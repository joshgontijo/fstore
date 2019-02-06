package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.fstore.core.Serializer;
import org.jgroups.Address;
import org.jgroups.Message;

import java.nio.ByteBuffer;

public class ClusterMessage {

    private static final Serializer<EventRecord> serializer = new EventSerializer();

    private final EventRecord message;
    private final Address sender;

    EventRecord reply;

    public ClusterMessage(EventRecord message, Address sender) {
        this.message = message;
        this.sender = sender;
    }

    public static ClusterMessage from(Message message) {
        if(message == null) {
            return noResponse();
        }
        EventRecord record = serializer.fromBytes(ByteBuffer.wrap(message.buffer()));
        return new ClusterMessage(record, message.src());
    }

    public static ClusterMessage noResponse() {
        return new ClusterMessage(null, null);
    }

    public void reply(EventRecord event) {
        this.reply = event;
    }

    public EventRecord message() {
        return message;
    }

    public boolean hasMessage() {
        return message != null;
    }

    public Address sender() {
        return sender;
    }

}
