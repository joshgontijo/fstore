package io.joshworks.eventry.projections.result;

import io.joshworks.eventry.projections.JsonEvent;

public class TaskError {
    public final String reason;
    public final long logPosition;
    public final String stream;
    public final int version;
    public final JsonEvent event;

    public TaskError(String reason, long logPosition, JsonEvent event) {
        this.reason = reason;
        this.logPosition = logPosition;
        this.event = event;
        if(event != null) {
            this.stream = event.stream;
            this.version = event.version;
        } else {
            this.stream = null;
            this.version = -1;
        }
    }
}
