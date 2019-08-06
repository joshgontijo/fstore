package io.joshworks.fstore.es.shared;

import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.util.Map;

public class JsonEvent {

    public final String type;
    public final long timestamp;
    public final String stream;
    public final int version;
    private final byte[] data;
    private final byte[] metadata;

    public JsonEvent(String type, long timestamp, String stream, int version, byte[] data, byte[] metadata) {
        this.type = type;
        this.timestamp = timestamp;
        this.stream = stream;
        this.version = version;
        this.data = data;
        this.metadata = metadata;
    }

    public Map<String, Object> asMap() {
        return JsonSerializer.toMap(new String(data));
    }

    public Map<String, Object> metadata() {
        return JsonSerializer.toMap(new String(metadata));
    }

    public <T> T as(Class<T> type) {
        return JsonSerializer.fromBytes(data, type);
    }

    @Override
    public String toString() {
        return "JsonEvent{" + "type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", stream='" + stream + '\'' +
                ", version=" + version +
                '}';
    }
}
