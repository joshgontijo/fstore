package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class EventData implements ClusterMessage {

    public static final int CODE = 99;

    private static final Serializer<EventRecord> serializer = new EventSerializer();

    public final EventRecord record;

    public EventData(EventRecord record) {
        this.record = record;
    }

    public EventData(ByteBuffer bb) {
        this.record = serializer.fromBytes(bb);
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer bb = serializer.toBytes(record);
        ByteBuffer envelope = ByteBuffer.allocate(Integer.BYTES + bb.remaining());
        envelope.putInt(CODE);
        envelope.put(bb);
        return envelope.flip().array();
    }

    @Override
    public int code() {
        return CODE;
    }
}
