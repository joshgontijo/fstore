package io.joshworks.fstore.es.shared;

import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;
import static io.joshworks.fstore.es.shared.utils.StringUtils.requireNonBlank;

public class EventRecord {

    public String stream;
    public String type;
    public int version;
    public long timestamp;
    public byte[] data;
    public byte[] metadata;

    public EventRecord() {
    }

    public EventRecord(String stream, String type, int version, long timestamp, byte[] data, byte[] metadata) {
        this.stream = requireNonBlank(stream, "Stream must be provided");
        this.type = requireNonBlank(type, "Type must be provided");
        this.version = version;
        this.timestamp = timestamp;
        this.data = data;
        this.metadata = metadata;
    }

    public static EventRecord create(String stream, String type, Map<String, Object> data) {
        return create(stream, type, JsonSerializer.toBytes(data));
    }

    public static EventRecord create(String stream, String type, byte[] data) {
        return create(stream, type, data, null);
    }

    public static EventRecord create(String stream, String type, byte[] data, byte[] metadata) {
        return new EventRecord(stream, type, NO_VERSION, -1, data, metadata);
    }

    public static EventRecord fromJson(String jsonEvent) {
        return JsonSerializer.fromJson(jsonEvent, EventRecord.class);
    }

    public Map<String, Object> asMap() {
        return JsonSerializer.toMap(new String(data, StandardCharsets.UTF_8));
    }

    public Map<String, Object> metadata() {
        if (metadata == null) {
            return Collections.emptyMap();
        }
        return JsonSerializer.toMap(new String(metadata, StandardCharsets.UTF_8));
    }

    public <T> T as(Class<T> type) {
        return JsonSerializer.fromJson(data, type);
    }

    public String dataAsJson() {
        return new String(data, StandardCharsets.UTF_8);
    }

    public JsonView asJson() {
        return new JsonView(stream, type, version, timestamp, asMap(), metadata());
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
                Arrays.equals(data, record.data) &&
                Arrays.equals(metadata, record.metadata);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(stream, type, version, timestamp);
        result = 31 * result + Arrays.hashCode(data);
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

    public static class JsonView {
        public final String stream;
        public final String type;
        public final int version;
        public final long timestamp;
        public final Map<String, Object> data;
        public final Map<String, Object> metadata;

        public JsonView(String stream, String type, int version, long timestamp, Map<String, Object> data, Map<String, Object> metadata) {
            this.stream = stream;
            this.type = type;
            this.version = version;
            this.timestamp = timestamp;
            this.data = data;
            this.metadata = metadata;
        }

        public String asString() {
            return JsonSerializer.toJson(this);
        }
    }


}
