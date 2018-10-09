package io.joshworks.eventry.projections.result;

public class Failure {
    public final String reason;
    public final long logPosition;
    public final String stream;
    public final int version;

    public Failure(String reason, long logPosition, String stream, int version) {
        this.reason = reason;
        this.logPosition = logPosition;
        this.stream = stream;
        this.version = version;
    }
}
