package io.joshworks.fstore.lsmtree.sstable;

import static io.joshworks.fstore.lsmtree.sstable.Entry.NO_TIMESTAMP;

public class IndexEntry<K extends Comparable<K>>  {

    final long timestamp;
    final K key;
    final long value;

    public IndexEntry(long timestamp, K key, long value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    private static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    boolean readable(long maxAge) {
        return !deletion() && !expired(maxAge);
    }

    private boolean expired(long maxAgeSeconds) {
        long now = nowSeconds();
        return maxAgeSeconds > 0 && timestamp > NO_TIMESTAMP && (now - timestamp > maxAgeSeconds);
    }

    private boolean deletion() {
        return value < 0;
    }
}
