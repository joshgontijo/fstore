package io.joshworks.eventry.projections;

import com.google.gson.reflect.TypeToken;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;

public class JsonEvent {

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
        Map<String, Object> data = JsonSerializer.toMap(new String(event.body));
        Map<String, Object> metadata = JsonSerializer.toMap(new String(event.metadata));
        return new JsonEvent(event.type, event.timestamp, event.stream, event.version, data, metadata);
    }

    public static JsonEvent fromMap(Map<String, Object> event) {
        String type = (String) event.get("type");
        String stream = (String) event.get("stream");
        long timestamp = (int) event.getOrDefault("timestamp", -1);
        int version = (int) event.getOrDefault("version", NO_VERSION);
        Map<String, Object> metadata = (Map<String, Object>) event.getOrDefault("metadata", new HashMap<String, Object>());
        Map<String, Object> data = (Map<String, Object>) event.getOrDefault("body", new HashMap<String, Object>());
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
