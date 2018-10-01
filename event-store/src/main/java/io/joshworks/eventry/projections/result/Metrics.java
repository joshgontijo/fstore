package io.joshworks.eventry.projections.result;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Metrics {

    public final Set<String> streams;
    public long processed;
    public long skipped;
    public long logPosition;

    public Metrics(Set<String> streams) {
        this.streams = Collections.unmodifiableSet(new HashSet<>(streams));
    }

    private Metrics(Set<String> streams, long processed, long skipped, long logPosition) {
        this(streams);
        this.processed = processed;
        this.skipped = skipped;
        this.logPosition = logPosition;
    }

    public Metrics copy() {
        return new Metrics(streams, processed, skipped, logPosition);
    }

}
