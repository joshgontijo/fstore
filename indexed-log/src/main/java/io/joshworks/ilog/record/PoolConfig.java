package io.joshworks.ilog.record;

import io.joshworks.fstore.core.util.Size;

import java.util.Set;

public class PoolConfig {

    int pollMaxSizeInBytes = Size.MB.ofInt(20);
    int batchSize = 100;
    boolean directBuffers;

    Set<Integer> poolStripes = Set.of(
            Size.BYTE.ofInt(512),
            Size.KB.ofInt(1),
            Size.KB.ofInt(2),
            Size.KB.ofInt(4),
            Size.KB.ofInt(8),
            Size.KB.ofInt(16),
            Size.KB.ofInt(32),
            Size.KB.ofInt(64),
            Size.KB.ofInt(256),
            Size.KB.ofInt(512),
            Size.MB.ofInt(1),
            Size.MB.ofInt(5));

    PoolConfig() {

    }

    public PoolConfig pollMaxSizeInBytes(int pollMaxSizeInBytes) {
        this.pollMaxSizeInBytes = pollMaxSizeInBytes;
        return this;
    }

    public PoolConfig batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public PoolConfig directBuffers(boolean directBuffers) {
        this.directBuffers = directBuffers;
        return this;
    }

    public PoolConfig poolStripes(Set<Integer> stripes) {
        if (stripes.isEmpty()) {
            throw new RuntimeException("Stripes cannot be null");
        }
        this.poolStripes = stripes;
        return this;
    }

    public RecordPool build() {
        StripedBufferPool pool = new StripedBufferPool(pollMaxSizeInBytes, directBuffers, poolStripes);
        return new RecordPool(pool, batchSize);
    }

}
