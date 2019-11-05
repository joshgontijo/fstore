package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.lsmtree.EntryType;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;

public class LogRecordSerializer<K, V> implements Serializer<LogRecord> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public LogRecordSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeTo(LogRecord data, ByteBuffer dst) {
        dst.putShort(data.type.code);
        dst.putLong(data.timestamp);
        switch (data.type) {
            case ADD:
                EntryAdded<K, V> added = (EntryAdded<K, V>) data;
                keySerializer.writeTo(added.key, dst);
                valueSerializer.writeTo(added.value, dst);
                break;
            case DELETE:
                EntryDeleted<K> deleted = (EntryDeleted<K>) data;
                keySerializer.writeTo(deleted.key, dst);
                break;
            case MEM_FLUSH_STARTED:
                IndexFlushedStarted flushedStarted = (IndexFlushedStarted) data;
                Serializers.VSTRING.writeTo(flushedStarted.token, dst);
                dst.putLong(flushedStarted.position);
                break;
            case MEM_FLUSHED:
                IndexFlushed flushed = (IndexFlushed) data;
                Serializers.VSTRING.writeTo(flushed.token, dst);
                break;
            default:
                throw new IllegalArgumentException("Invalid record type");
        }
    }

    @Override
    public LogRecord fromBytes(ByteBuffer buffer) {
        short code = buffer.getShort();
        long timestamp = buffer.getLong();
        switch (EntryType.of(code)) {
            case ADD:
                K k = keySerializer.fromBytes(buffer);
                V v = valueSerializer.fromBytes(buffer);
                return new EntryAdded<>(timestamp, k, v);
            case DELETE:
                K k1 = keySerializer.fromBytes(buffer);
                return new EntryDeleted<>(timestamp, k1);
            case MEM_FLUSH_STARTED:
                String token = Serializers.VSTRING.fromBytes(buffer);
                long position = buffer.getLong();
                return new IndexFlushedStarted(timestamp, position, token);
            case MEM_FLUSHED:
                String token1 = Serializers.VSTRING.fromBytes(buffer);
                return new IndexFlushed(timestamp, token1);
            default:
                throw new IllegalStateException("Unknown record type: " + code);
        }
    }
}
