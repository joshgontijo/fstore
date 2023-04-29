package io.joshworks.es2.sstable;

import io.joshworks.fstore.core.util.Memory;

public class CompactionProfile {
    int blockSize = Memory.PAGE_SIZE * 2;
    BlockCodec codec = BlockCodec.LZ4;
    int compactionThreshold = 2;
    double bloomFilterFalsePositive = 0.05;

    public CompactionProfile() {

    }

    CompactionProfile copy() {
        CompactionProfile copy = new CompactionProfile();
        copy.blockSize = this.blockSize;
        copy.compactionThreshold = this.compactionThreshold;
        copy.codec = this.codec;
        copy.bloomFilterFalsePositive = this.bloomFilterFalsePositive;
        return copy;
    }

    public CompactionProfile dataBlockSize(int dataBlockSize) {
        this.blockSize = dataBlockSize;
        return this;
    }

    public CompactionProfile threshold(int compactionThreshold) {
        this.compactionThreshold = compactionThreshold;
        return this;
    }

    public CompactionProfile codec(BlockCodec codec) {
        this.codec = codec;
        return this;
    }

    public CompactionProfile bloomFilterFalsePositive(double bloomFilterFalsePositive) {
        if (bloomFilterFalsePositive <= 0) {
            throw new RuntimeException("Value must be greater than zero");
        }
        this.bloomFilterFalsePositive = bloomFilterFalsePositive;
        return this;
    }

    public int compactionThreshold() {
        return compactionThreshold;
    }
}
