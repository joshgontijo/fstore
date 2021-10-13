package io.joshworks.sdb.http;

import java.util.Map;

public class HttpEvent {
    public final int version;
    public final long timestamp;
    public final String type;
    public final Map<String, Object> data;

    public HttpEvent(int version, long timestamp, String type, Map<String, Object> data) {
        this.version = version;
        this.timestamp = timestamp;
        this.type = type;
        this.data = data;
    }
}
