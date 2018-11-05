package io.joshworks.eventry.log;

import io.joshworks.eventry.MapRecordSerializer;
import io.joshworks.eventry.data.Constant;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class EventRecord {

    public static final String STREAM_VERSION_SEPARATOR = "@";

    private static final Serializer<Map<String, Object>> serializer = new MapRecordSerializer();

    public final String stream;
    public final String type;
    public final int version;
    public final long timestamp;
    public final byte[] data;
    public final byte[] metadata;

    public EventRecord(String stream, String type, int version, long timestamp, byte[] data, byte[] metadata) {
        StringUtils.requireNonBlank(stream, "Stream must be provided");
        StringUtils.requireNonBlank(type, "Type must be provided");
        this.stream = stream;
        this.type = type;
        this.version = version;
        this.timestamp = timestamp;
        this.data = data;
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
        return new String(data, StandardCharsets.UTF_8);
    }

    public String eventId() {
        return stream + STREAM_VERSION_SEPARATOR + version;
    }

    public boolean isSystemEvent() {
        return type.startsWith(Constant.SYSTEM_PREFIX);
    }

    public boolean isLinkToEvent() {
        return LinkTo.TYPE.equals(type);
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
