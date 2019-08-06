package io.joshworks.eventry.log;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;
import static io.joshworks.fstore.es.shared.utils.StringUtils.requireNonBlank;

public class EventRecord {

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
        return create(stream, type, KryoStoreSerializer.serialize(data, Map.class));
    }

    public static EventRecord create(String stream, String type, byte[] data) {
        return create(stream, type, data, null);
    }

    public static EventRecord create(String stream, String type, byte[] data, byte[] metadata) {
        return new EventRecord(stream, type, NO_VERSION, -1, data, metadata);
    }

    public String eventId() {
        return EventId.toString(stream, version);
    }

    public boolean isSystemEvent() {
        return type.startsWith(EventId.SYSTEM_PREFIX);
    }

    public boolean isLinkToEvent() {
        return LinkTo.TYPE.equals(type);
    }

    public long hash() {
        return EventId.hash(stream);
    }

    public EventId streamName() {
        return EventId.of(stream, version);
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
