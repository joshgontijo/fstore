package io.joshworks.eventry.projections;

import com.google.gson.reflect.TypeToken;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.util.Map;

public class JsonEvent {

    //    private static final Gson gson = new Gson();
    private static final Serializer<Map<String, Object>> jsonSerializer = JsonSerializer.of(new TypeToken<Map<String, Object>>() {
    }.getType());
//    private static final Serializer<Map<String, Object>> mapSerializer = new MapRecordSerializer();

    public final String type;
    public final long timestamp;
    public final String stream;
    public final int version;
    public final boolean systemEvent;

    public final Map<String, Object> data;
    public final Map<String, Object> metadata;


    private JsonEvent(String type, long timestamp, String stream, int version, Map<String, Object> data, Map<String, Object> metadata, boolean systemEvent) {
        this.type = type;
        this.timestamp = timestamp;
        this.stream = stream;
        this.version = version;
        this.data = data;
        this.metadata = metadata;
        this.systemEvent = systemEvent;
    }

    public static JsonEvent from(EventRecord event) {
        Map<String, Object> data = JsonSerializer.toMap(new String(event.body));
        Map<String, Object> metadata = JsonSerializer.toMap(new String(event.metadata));
        return new JsonEvent(event.type, event.timestamp, event.stream, event.version, data, metadata, event.isSystemEvent());
    }

    public static JsonEvent fromMap(Map<String, Object> event) {
        String type = (String) event.get("type");
        long timestamp = (int) event.get("timestamp");
        String stream = (String) event.get("stream");
        int version = (int) event.get("version");
        Map<String, Object> metadata = (Map<String, Object>) event.get("metadata");
        Map<String, Object> data = (Map<String, Object>) event.get("body");
        return new JsonEvent(type, timestamp, stream, version, data, metadata, type.startsWith(StreamName.SYSTEM_PREFIX));
    }

    public EventRecord toEvent() {
        return new EventRecord(stream, type, version, timestamp, JsonSerializer.toJsonBytes(data), JsonSerializer.toJsonBytes(metadata));
    }

    public String toJson() {
        return data.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
