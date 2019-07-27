package io.joshworks.fstore.log.segment.block;

public class BlockSegmentInfo {

    private final int blockSize;
    private long entries;
    private long uncompressedSize;
    private long compressedSize;

    public BlockSegmentInfo(int blockSize) {
        this.blockSize = blockSize;
    }

    public void entries(long entries) {
        this.entries = entries;
    }

    public void uncompressedSize(long uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
    }

    public void addEntryCount(int delta) {
        entries += delta;
    }

    public int blockSize() {
        return blockSize;
    }

    public long entries() {
        return entries;
    }

    public long uncompressedSize() {
        return uncompressedSize;
    }

    public long compressedSize() {
        return compressedSize;
    }

    public void addUncompressedSize(int uncompressedSize) {
        this.uncompressedSize += uncompressedSize;
    }

    public void addCompressedSize(int compressedSize) {
        this.compressedSize += compressedSize;
    }

    @Override
    public String toString() {
        return "BlockSegmentInfo{" + "blockSize=" + blockSize +
                ", entries=" + entries +
                ", uncompressedSize=" + uncompressedSize +
                ", compressedSize=" + compressedSize +
                '}';
    }
}
