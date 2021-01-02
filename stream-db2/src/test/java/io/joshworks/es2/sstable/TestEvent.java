package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.StreamHasher;
import io.joshworks.fstore.core.util.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class TestEvent {

    public final long stream;
    public final int version;
    public final long sequence;
    public final long timestamp;
    public final long eventTimestamp;
    public final String type;
    public final String data;

    private TestEvent(long stream, int version, long sequence, long timestamp, long eventTimestamp, String type, String data) {
        this.stream = stream;
        this.version = version;
        this.sequence = sequence;
        this.timestamp = timestamp;
        this.eventTimestamp = eventTimestamp;
        this.type = type;
        this.data = data;
    }

    public static TestEvent create(String stream, int version, long sequence, String type, String data) {
        long ts = System.currentTimeMillis();
        return new TestEvent(StreamHasher.hash(stream), version, sequence, ts, ts, type, data);
    }

    public static TestEvent from(ByteBuffer event) {
        return new TestEvent(
                Event.stream(event),
                Event.version(event),
                Event.sequence(event),
                Event.timestamp(event),
                Event.eventTimestamp(event),
                Event.eventType(event),
                Event.dataString(event));
    }

    public ByteBuffer serialize() {
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
        byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
        int recSize = dataBytes.length + typeBytes.length + Event.HEADER_BYTES;

        ByteBuffer dst = ByteBuffer.allocate(recSize);
        int bpos = dst.position();

        byte[] evTypeBytes = StringUtils.toUtf8Bytes(type);
        dst.putInt(recSize);
        dst.putLong(stream);
        dst.putInt(version);
        dst.putLong(sequence);

        long ts = System.currentTimeMillis();
        dst.putLong(ts);
        dst.putLong(ts);

        dst.putShort((short) evTypeBytes.length);
        dst.putInt(dataBytes.length);

        dst.put(evTypeBytes);
        dst.put(dataBytes);

        int copied = (dst.position() - bpos);

        assert copied == recSize;
        return dst.flip();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestEvent testEvent = (TestEvent) o;
        return stream == testEvent.stream &&
                version == testEvent.version &&
                sequence == testEvent.sequence &&
                Objects.equals(type, testEvent.type) &&
                Objects.equals(data, testEvent.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream, version, sequence, type, data);
    }

    @Override
    public String toString() {
        return "TestEvent{" +
                "stream=" + stream +
                ", version=" + version +
                ", sequence=" + sequence +
                ", type='" + type + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
