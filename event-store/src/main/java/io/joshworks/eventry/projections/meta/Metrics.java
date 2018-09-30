package io.joshworks.eventry.projections.meta;

public class Metrics {
    public long processed;
    public long skipped;
    public long logPosition;

    public Metrics() {
    }

    private Metrics(long processed, long skipped, long logPosition) {
        this.processed = processed;
        this.skipped = skipped;
        this.logPosition = logPosition;
    }
    
    public Metrics copy() {
        return new Metrics(processed, skipped, logPosition);
    }
    
}
