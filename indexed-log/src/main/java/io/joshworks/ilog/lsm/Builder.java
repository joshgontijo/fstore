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
    private int memTableEntries = 500000;
    private int blockSize = Size.KB.ofInt(4);
    private long maxAge = -1;
    private Codec codec = Codec.noCompression();
    private int compactionThreads = 1;
    private int compactionThreshold = 2;
    private int memTableMaxSizeInBytes = Size.MB.ofInt(100);

    public Builder(File root, KeyComparator comparator) {
        this.root = root;
        this.comparator = comparator;
    }

    public Builder memTable(int memTableEntries, int maxSizeInBytes) {
        if (memTableEntries <= 0) {
            throw new IllegalArgumentException("memTableEntries must be a positive number");
        }
        long min = Size.MB.of(1);
//        if (maxSizeInBytes <= min) {
//            throw new IllegalArgumentException("maxSizeInBytes must be at least " + min);
//        }
        this.memTableMaxSizeInBytes = maxSizeInBytes;
        this.memTableEntries = memTableEntries;
        return this;
    }

    public void compactionThreshold(int compactionThreshold) {
        this.compactionThreshold = compactionThreshold;
    }

    public void compactionThreads(int compactionThreads) {
        if (compactionThreads <= 0) {
            throw new IllegalArgumentException("Values must be greater than zero");
        }
        this.compactionThreads = compactionThreads;
    }

    public void blockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public void maxAge(long maxAge) {
        this.maxAge = maxAge;
    }

    public void codec(Codec codec) {
        this.codec = requireNonNull(codec);
    }

    public Lsm open() {
        try {
            return new Lsm(root,
                    comparator,
                    memTableEntries,
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
