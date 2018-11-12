package io.joshworks.eventry.projections.result;

import io.joshworks.eventry.projections.JsonEvent;

public class TaskError {
    public final String reason;
    public final long logPosition;
    public final String stream;
    public final int version;
    public final JsonEvent event;

    public TaskError(String reason, long logPosition, String stream, int version, JsonEvent event) {
        this.reason = reason;
        this.logPosition = logPosition;
        this.stream = stream;
        this.version = version;
        this.event = event;
    }
}
