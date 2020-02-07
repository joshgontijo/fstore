package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.ilog.index.KeyComparator;

import java.io.File;

import static java.util.Objects.requireNonNull;

public class Builder {
    private final File root;
    private final KeyComparator comparator;
    private int blockSize = Size.KB.ofInt(4);
    private long maxAge = -1;
    private Codec codec = Codec.noCompression();

    private int compactionThreads = 1;
    private int compactionThreshold = 2;

    private int memTableMaxSizeInBytes = Size.MB.ofInt(5);
    private int memTableMaxEntries = 500000;
    private boolean memTableDirect;

    public Builder(File root, KeyComparator comparator) {
        this.root = root;
        this.comparator = comparator;
    }

    public Builder memTable(int maxEntries, int maxSize, boolean direct) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be a positive number");
        }
        this.memTableMaxSizeInBytes = maxSize;
        this.memTableMaxEntries = maxEntries;
        this.memTableDirect = direct;
        return this;
    }

    public Builder compactionThreshold(int compactionThreshold) {
        this.compactionThreshold = compactionThreshold;
        return this;
    }

    public Builder compactionThreads(int compactionThreads) {
        this.compactionThreads = compactionThreads;
        return this;
    }

    public Builder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public Builder maxAge(long maxAge) {
        this.maxAge = maxAge;
        return this;
    }

    public Builder codec(Codec codec) {
        this.codec = requireNonNull(codec);
        return this;
    }

    public Lsm open() {
        try {
            return new Lsm(root,
                    comparator,
                    memTableMaxSizeInBytes,
                    memTableMaxEntries,
                    memTableDirect,
                    blockSize,
                    maxAge,
                    compactionThreads,
                    compactionThreshold,
                    codec);

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to create LSM", e);
        }
    }
}
