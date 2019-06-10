package io.joshworks.fstore.log.segment.header;

public final class MergeSection {
    public final long timestamp;

    public MergeSection(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "{" +
                "timestamp=" + timestamp +
                '}';
    }
}
