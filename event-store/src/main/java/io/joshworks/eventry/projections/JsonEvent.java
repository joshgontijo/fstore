package io.joshworks.eventry.projections;

import io.joshworks.eventry.MapRecordSerializer;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class JsonEvent {

    //    private static final Gson gson = new Gson();
//    private static final Serializer<Map<String, Object>> jsonSerializer = JsonSerializer.of(new TypeToken<Map<String, Object>>(){}.getType());
    private static final Serializer<Map<String, Object>> jsonSerializer = new MapRecordSerializer();

    public final String type;
    public final long timestamp;
    public final String stream;
    public final int version;

    public final Map<String, Object> metadata;
    public final Map<String, Object> data;

    private JsonEvent(EventRecord event) {
        this(event.type, event.timestamp, event.stream, event.version, jsonSerializer.fromBytes(ByteBuffer.wrap(event.metadata)), jsonSerializer.fromBytes(ByteBuffer.wrap(event.data)));
    }

    private JsonEvent(String type, long timestamp, String stream, int version, Map<String, Object> metadata, Map<String, Object> data) {
        this.type = type;
        this.timestamp = timestamp;
        this.stream = stream;
        this.version = version;
        this.metadata = metadata;
        this.data = data;
    }

    public static JsonEvent from(EventRecord event) {
        return new JsonEvent(event);
    }

    public static JsonEvent fromMap(Map<String, Object> event) {
        String type = (String) event.get("type");
        long timestamp = (int) event.get("timestamp");
        String stream = (String) event.get("stream");
        int version = (int) event.get("version");
        Map<String, Object> metadata = (Map<String, Object>) event.get("metadata");
        Map<String, Object> data = (Map<String, Object>) event.get("data");
        return new JsonEvent(type, timestamp, stream, version, metadata, data);
    }

    public EventRecord toEvent() {
        return new EventRecord(stream, type, version, timestamp, jsonSerializer.toBytes(data).array(), jsonSerializer.toBytes(metadata).array());
    }

    public Map<String, Object> toMap() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("type", type);
        eventMap.put("timestamp", timestamp);
        eventMap.put("stream", stream);
        eventMap.put("version", version);
        eventMap.put("data", data);
        eventMap.put("metadata", metadata);

        return eventMap;
    }

    public String toJson() {
        return data.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
