package io.joshworks.ilog.record;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.ilog.index.RowKey;

import java.util.Set;

public class PoolConfig {

    final String poolName;
    final RowKey rowKey;
    int maxRecordSize = Size.MB.ofInt(1);
    int batchSize = 100;
    boolean directBuffers;
    int readBufferSize = Size.KB.ofInt(8);

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

    PoolConfig(String poolName, RowKey rowKey) {
        this.poolName = poolName;
        this.rowKey = rowKey;
    }

    public static PoolConfig create(String poolName, RowKey rowKey) {
        return new PoolConfig(poolName, rowKey);
    }

    public PoolConfig maxRecordSize(int maxRecordSize) {
        this.maxRecordSize = maxRecordSize;
        return this;
    }

    public PoolConfig batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public PoolConfig readBufferSize(int readBufferSize) {
        this.readBufferSize = readBufferSize;
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
}
