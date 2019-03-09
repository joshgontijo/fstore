package io.joshworks.eventry.projection;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import io.joshworks.fstore.serializer.kryo.KryoSerializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static io.joshworks.eventry.log.EventRecord.NO_VERSION;

public class JsonEvent {

    private static final Serializer<Map<String, Object>> serializer = KryoSerializer.untyped();

    public final String type;
    public final long timestamp;
    public final String stream;
    public final int version;

    public final Map<String, Object> body;
    public final Map<String, Object> metadata;

    private JsonEvent(String type, long timestamp, String stream, int version, Map<String, Object> body, Map<String, Object> metadata) {
        this.type = type;
        this.timestamp = timestamp;
        this.stream = stream;
        this.version = version;
        this.body = body;
        this.metadata = metadata;
    }

    public static JsonEvent from(EventRecord event) {
        Map<String, Object> data = serializer.fromBytes(ByteBuffer.wrap(event.body));
        Map<String, Object> metadata = event.metadata == null ? new HashMap<>() : serializer.fromBytes(ByteBuffer.wrap(event.metadata));
        return new JsonEvent(event.type, event.timestamp, event.stream, event.version, data, metadata);
    }

    public static JsonEvent fromMap(Map<String, Object> event) {
        String type = (String) event.get("type");
        String stream = (String) event.get("stream");
        long timestamp = (int) event.getOrDefault("timestamp", -1);
        int version = (int) event.getOrDefault("version", NO_VERSION);
        Map<String, Object> data = (Map<String, Object>) event.getOrDefault("body", new HashMap<>());
        Map<String, Object> metadata = (Map<String, Object>) event.getOrDefault("metadata", new HashMap<>());
        return new JsonEvent(type, timestamp, stream, version, data, metadata);
    }

    public EventRecord toEvent() {
        return new EventRecord(stream, type, version, timestamp, JsonSerializer.toJsonBytes(body), JsonSerializer.toJsonBytes(metadata));
    }

    public String dataAsJson() {
        return JsonSerializer.toJson(body);
    }

    public String toJson() {
        return JsonSerializer.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
