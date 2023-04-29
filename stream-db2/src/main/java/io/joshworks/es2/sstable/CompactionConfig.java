package io.joshworks.es2.sstable;

import io.joshworks.fstore.core.util.Memory;

public class CompactionConfig {

    int threads = 2;
    int levelThreshold = 4;
    int compactionThreshold = 3;

    CompactionProfile lowConfig = new CompactionProfile()
            .codec(BlockCodec.LZ4)
            .bloomFilterFalsePositive(0.01)
            .dataBlockSize(Memory.PAGE_SIZE)
            .threshold(2);

    private CompactionProfile highConfig = new CompactionProfile()
            .codec(BlockCodec.ZLIB)
            .bloomFilterFalsePositive(0.1)
            .dataBlockSize(Memory.PAGE_SIZE * 2)
            .threshold(2);

    public CompactionConfig() {
    }

    public CompactionConfig threads(int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("Compaction threads must be greater than zero");
        }
        this.threads = threads;
        return this;
    }

    public CompactionProfile profileForLevel(int level) {
        return level >= levelThreshold ? this.highConfig : this.lowConfig;
    }

    public int threads() {
        return threads;
    }

    public CompactionConfig levelThreshold(int levelThreshold) {
        if (levelThreshold < 1) {
            throw new IllegalArgumentException("level threshold must be greater than zero");
        }
        this.levelThreshold = levelThreshold;
        return this;
    }

    public CompactionConfig threshold(int compactionThreshold) {
        if (levelThreshold <= 1) {
            throw new IllegalArgumentException("level threshold must be greater than one");
        }
        this.compactionThreshold = compactionThreshold;
        return this;
    }

    public CompactionConfig low(CompactionProfile lowConfig) {
        this.lowConfig = lowConfig;
        return this;
    }

    public CompactionConfig high(CompactionProfile highConfig) {
        this.highConfig = highConfig;
        return this;
    }

    CompactionConfig copy() {
        return new CompactionConfig()
                .threshold(compactionThreshold)
                .levelThreshold(levelThreshold)
                .low(lowConfig.copy())
                .high(highConfig.copy());
    }

}
