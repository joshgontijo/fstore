package io.joshworks.eventry.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class EventSerializer implements Serializer<EventRecord> {

    private final Serializer<String> strSerializer = new VStringSerializer();

    @Override
    public void writeTo(EventRecord data, ByteBuffer dst) {
        strSerializer.writeTo(data.type, dst);
        strSerializer.writeTo(data.stream, dst);
        dst.putInt(data.version);
        dst.putLong(data.timestamp);

        dst.putInt(data.data.length);
        dst.put(data.data);

        int metadataLen = data.metadata == null ? 0 : data.metadata.length;
        dst.putInt(metadataLen);
        if (metadataLen > 0) {
            dst.put(data.metadata);
        }
    }


    @Override
    public EventRecord fromBytes(ByteBuffer buffer) {
        String type = strSerializer.fromBytes(buffer);
        String stream = strSerializer.fromBytes(buffer);
        int version = buffer.getInt();
        long timestamp = buffer.getLong();

        int dataLength = buffer.getInt();
        byte[] data = new byte[dataLength];
        buffer.get(data);

        int metadataLength = buffer.getInt();
        byte[] metadata = null;
        if (metadataLength > 0) {
            metadata = new byte[metadataLength];
            buffer.get(metadata);
        }

        return new EventRecord(stream, type, version, timestamp, data, metadata);
    }

}
