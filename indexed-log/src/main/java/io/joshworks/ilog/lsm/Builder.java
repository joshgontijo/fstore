package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.PoolConfig;
import io.joshworks.ilog.record.RecordPool;

import java.io.File;

public class Builder {
    private final File root;
    private final RowKey comparator;
    private long maxAge = -1;

    private int compactionThreshold = 2;

    private int memTableMaxEntries = 500000;
    private int memTableMaxSize = Size.MB.ofInt(20);
    private boolean memTableDirectBuffers = false;
    private final PoolConfig pool = RecordPool.create();

    public Builder(File root, RowKey comparator) {
        this.root = root;
        this.comparator = comparator;
    }

    public Builder memTable(int maxEntries, int maxSize, boolean direct) {
        this.memTableMaxSize = maxSize;
        this.memTableMaxEntries = maxEntries;
        this.memTableDirectBuffers = direct;
        return this;
    }

    public Builder compactionThreshold(int compactionThreshold) {
        this.compactionThreshold = compactionThreshold;
        return this;
    }

    public Builder maxAge(long maxAge) {
        this.maxAge = maxAge;
        return this;
    }

    /**
     * Create a new sparse LSMTree
     */
    public SparseLsm sparse(Codec codec, int blockSize) {
        return new SparseLsm(
                root,
                pool.build(),
                comparator,
                memTableMaxEntries,
                memTableMaxSize,
                memTableDirectBuffers,
                maxAge,
                compactionThreshold,
                blockSize,
                codec);
    }

    /**
     * Create a new dense LSMTree
     */
    public Lsm open() {
        try {
            return new Lsm(
                    root,
                    pool.build(),
                    comparator,
                    memTableMaxEntries,
                    memTableMaxSize,
                    memTableDirectBuffers,
                    maxAge,
                    compactionThreshold);

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to create LSM", e);
        }
    }
}
