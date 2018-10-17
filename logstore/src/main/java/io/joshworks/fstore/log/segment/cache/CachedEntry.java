package io.joshworks.fstore.log.segment.cache;

import java.nio.ByteBuffer;

final class CachedEntry {
    final long timestamp = System.currentTimeMillis();
    final ByteBuffer buffer;

    CachedEntry(ByteBuffer buffer) {
        this.buffer = buffer;
    }
}
