package io.joshworks.eventry.log.cache;

import java.nio.ByteBuffer;

final class CachedEntry {

    private final ByteBuffer buffer;
    private final long created = System.currentTimeMillis();
    private long lastRead = System.currentTimeMillis();

    CachedEntry(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public ByteBuffer get() {
        lastRead = System.currentTimeMillis();
        return buffer.asReadOnlyBuffer().clear();
    }

    public long created() {
        return created;
    }

    public long lastRead() {
        return lastRead;
    }
}
