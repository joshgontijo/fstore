package io.joshworks.eventry.log;

import com.google.gson.reflect.TypeToken;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.joshworks.eventry.utils.StringUtils.requireNonBlank;

public class EventRecord {

//    private static final Serializer<Map<String, Object>> serializer = new MapRecordSerializer();
    private static final Serializer<Map<String, Object>> serializer = JsonSerializer.of(new TypeToken<Map<String, Object>>(){}.getType());

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

    public static EventRecord create(String stream, String type, String data) {
        return create(stream, type, StringUtils.toUtf8Bytes(data));
    }

    public static EventRecord create(String stream, String type, String data, String metadata) {
        return create(stream, type, StringUtils.toUtf8Bytes(data), StringUtils.toUtf8Bytes(metadata));
    }

    public static EventRecord create(String stream, String type, byte[] data) {
        return create(stream, type, data, new byte[0]);
    }

    public static EventRecord create(String stream, String type, byte[] data, byte[] metadata) {
        return new EventRecord(stream, type, -1, -1, data, metadata);
    }

    //TODO use in the response
    public String dataAsString() {
        return new String(body, StandardCharsets.UTF_8);
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

    public StreamName streamName() {
        return StreamName.of(stream, version);
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
