package io.joshworks.fstore.es.shared;

import java.util.Map;

public class JsonEvent {

    public final String type;
    public final long timestamp;
    public final String stream;
    public final int version;
    public final Map<String, Object> data;
    public final Map<String, Object> metadata;

    public JsonEvent(String type, long timestamp, String stream, int version, Map<String, Object> data, Map<String, Object> metadata) {
        this.type = type;
        this.timestamp = timestamp;
        this.stream = stream;
        this.version = version;
        this.data = data;
        this.metadata = metadata;
    }

}
