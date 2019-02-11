package io.joshworks.eventry.server.cluster.commands;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class Append implements ClusterMessage {

    public static final int CODE = 0;

    private static final Serializer<EventRecord> serializer = new EventSerializer();

    public final int expectedVersion;
    public final EventRecord record;

    public Append(EventRecord record, int expectedVersion) {
        this.record = record;
        this.expectedVersion = expectedVersion;
    }

    public Append(ByteBuffer bb) {
        this.expectedVersion = bb.getInt();
        this.record = serializer.fromBytes(bb);
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer bb = serializer.toBytes(record);
        ByteBuffer envelope = ByteBuffer.allocate(Integer.BYTES + bb.remaining());
        envelope.putInt(CODE);
        envelope.putInt(expectedVersion);
        envelope.put(bb);
        return envelope.flip().array();
    }
}
