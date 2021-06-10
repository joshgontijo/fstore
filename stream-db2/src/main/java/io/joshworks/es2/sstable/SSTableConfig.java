package io.joshworks.es2.sstable;

import io.joshworks.fstore.core.util.Memory;

public class SSTableConfig {

    int dataBlockSize = Memory.PAGE_SIZE;
    BlockCodec codec = BlockCodec.SNAPPY;
    int compactionThreshold = 3;
    double bloomFilterFalsePositive = 0.01;

    private SSTableConfig() {

    }

    private SSTableConfig(
            int dataBlockSize,
            int compactionThreshold,
            BlockCodec codec,
            double bloomFilterFalsePositive) {

        this.dataBlockSize = dataBlockSize;
        this.compactionThreshold = compactionThreshold;
        this.codec = codec;
        this.bloomFilterFalsePositive = bloomFilterFalsePositive;
    }

    public static SSTableConfig create() {
        return new SSTableConfig();
    }

    public void dataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
    }

    public void compactionThreshold(int compactionThreshold) {
        this.compactionThreshold = compactionThreshold;
    }

    public void codec(BlockCodec codec) {
        this.codec = codec;
    }

    public void bloomFilterFalsePositive(double bloomFilterFalsePositive) {
        this.bloomFilterFalsePositive = bloomFilterFalsePositive;
    }

    SSTableConfig copy() {
        return new SSTableConfig(
                dataBlockSize,
                compactionThreshold,
                codec,
                bloomFilterFalsePositive);
    }

}
