package io.joshworks.es2.sstable;

import io.joshworks.fstore.core.util.Memory;

public class SSTableConfig {

    int levelThreshold = 4;
    int compactionThreshold = 3;

    Config lowConfig = new Config()
            .codec(BlockCodec.LZ4_HIGH)
            .bloomFilterFalsePositive(0.01)
            .dataBlockSize(Memory.PAGE_SIZE)
            .compactionThreshold(3);

    Config highConfig = new Config()
            .codec(BlockCodec.LZ4_HIGH)
            .bloomFilterFalsePositive(0.1)
            .dataBlockSize(Memory.PAGE_SIZE * 2)
            .compactionThreshold(3);

    public SSTableConfig() {
    }

    public SSTableConfig levelThreshold(int levelThreshold) {
        if (levelThreshold < 1) {
            throw new IllegalArgumentException("level threshold must be greater than zero");
        }
        this.levelThreshold = levelThreshold;
        return this;
    }

    public SSTableConfig compactionThreshold(int compactionThreshold) {
        if (levelThreshold <= 1) {
            throw new IllegalArgumentException("level threshold must be greater than one");
        }
        this.compactionThreshold = compactionThreshold;
        return this;
    }

    public SSTableConfig low(Config lowConfig) {
        this.lowConfig = lowConfig;
        return this;
    }

    public SSTableConfig high(Config highConfig) {
        this.highConfig = highConfig;
        return this;
    }

    SSTableConfig copy() {
        return new SSTableConfig()
                .levelThreshold(levelThreshold)
                .low(lowConfig.copy())
                .high(highConfig.copy());
    }

    public static class Config {
        int blockSize;
        BlockCodec codec;
        int compactionThreshold;
        double bloomFilterFalsePositive;

        private Config() {

        }

        private Config copy() {
            Config copy = new Config();
            copy.blockSize = this.blockSize;
            copy.compactionThreshold = this.compactionThreshold;
            copy.codec = this.codec;
            copy.bloomFilterFalsePositive = this.bloomFilterFalsePositive;
            return copy;
        }

        public Config dataBlockSize(int dataBlockSize) {
            this.blockSize = dataBlockSize;
            return this;
        }

        public Config compactionThreshold(int compactionThreshold) {
            this.compactionThreshold = compactionThreshold;
            return this;
        }

        public Config codec(BlockCodec codec) {
            this.codec = codec;
            return this;
        }

        public Config bloomFilterFalsePositive(double bloomFilterFalsePositive) {
            this.bloomFilterFalsePositive = bloomFilterFalsePositive;
            return this;
        }
    }

}
