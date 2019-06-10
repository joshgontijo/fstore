package io.joshworks.fstore.log.segment.header;

public final class DeletedSection {
    public final long timestamp;

    public DeletedSection(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "{" +
                "timestamp=" + timestamp +
                '}';
    }
}
