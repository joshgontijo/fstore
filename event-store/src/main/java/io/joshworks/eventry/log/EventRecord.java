package io.joshworks.eventry.log;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static io.joshworks.eventry.utils.StringUtils.requireNonBlank;

public class EventRecord {

    private static final Serializer<Map<String, Object>> serializer = KryoStoreSerializer.of();

    public static final int NO_VERSION = -1;
    public static final int NO_EXPECTED_VERSION = -2;

    public final String stream;
    public final String type;
    public final int version;
    public final long timestamp;
    public final byte[] body;
    public final byte[] metadata;

    public EventRecord(String stream, String type, int version, long timestamp, byte[] body, byte[] metadata) {
        this.stream = requireNonBlank(stream, "Stream must be provided");
        this.type = requireNonBlank(type, "Type must be provided");
        this.version = version;
        this.timestamp = timestamp;
        this.body = body;
        this.metadata = metadata;
    }

    public static EventRecord create(String stream, String type, Map<String, Object> data) {
        return create(stream, type, serializer.toBytes(data).array());
    }

    public static EventRecord create(String stream, String type, byte[] data) {
        return create(stream, type, data, null);
    }

    public static EventRecord create(String stream, String type, byte[] data, byte[] metadata) {
        return new EventRecord(stream, type, NO_VERSION, -1, data, metadata);
    }

    public String eventId() {
        return StreamName.toString(stream, version);
    }

    public boolean isSystemEvent() {
        return type.startsWith(StreamName.SYSTEM_PREFIX);
    }

    public boolean isLinkToEvent() {
        return LinkTo.TYPE.equals(type);
    }

    public long hash() {
        return StreamName.hash(stream);
    }

    public StreamName streamName() {
        return StreamName.of(stream, version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventRecord record = (EventRecord) o;
        return version == record.version &&
                timestamp == record.timestamp &&
                Objects.equals(stream, record.stream) &&
                Objects.equals(type, record.type) &&
                Arrays.equals(body, record.body) &&
                Arrays.equals(metadata, record.metadata);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(stream, type, version, timestamp);
        result = 31 * result + Arrays.hashCode(body);
        result = 31 * result + Arrays.hashCode(metadata);
        return result;
    }

    @Override
    public String toString() {
        return "EventRecord{" + "stream='" + stream + '\'' +
                ", type='" + type + '\'' +
                ", version=" + version +
                ", timestamp=" + timestamp +
                '}';
    }

}
