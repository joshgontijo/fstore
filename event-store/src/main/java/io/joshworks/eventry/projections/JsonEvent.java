package io.joshworks.eventry.projections;

import io.joshworks.eventry.MapRecordSerializer;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class JsonEvent {

    //    private static final Gson gson = new Gson();
//    private static final Serializer<Map<String, Object>> jsonSerializer = JsonSerializer.of(new TypeToken<Map<String, Object>>(){}.getType());
    private static final Serializer<Map<String, Object>> mapSerializer = new MapRecordSerializer();

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
        Map<String, Object> data;
        Map<String, Object> metadata;
        if(event.isSystemEvent()) { //system event uses json serializer
            data = JsonSerializer.toMap(new String(event.data));
            metadata = JsonSerializer.toMap(new String(event.metadata));
        } else { //user events uses custom map serializer
            data = mapSerializer.fromBytes(ByteBuffer.wrap(event.data));
            metadata = mapSerializer.fromBytes(ByteBuffer.wrap(event.metadata));
        }
        return new JsonEvent(event.type, event.timestamp, event.stream, event.version, metadata, data, event.isSystemEvent());
    }

    public EventRecord toEvent() {
        if(systemEvent) {
            return new EventRecord(stream, type, version, timestamp, JsonSerializer.toJsonBytes(data), JsonSerializer.toJsonBytes(data));
        }
        return new EventRecord(stream, type, version, timestamp, mapSerializer.toBytes(data).array(), mapSerializer.toBytes(metadata).array());
    }

    public String toJson() {
        return data.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
