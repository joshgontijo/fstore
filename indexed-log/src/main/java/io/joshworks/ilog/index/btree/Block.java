package io.joshworks.ilog.index.btree;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.mmap.MappedRegion;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record;

import java.nio.ByteBuffer;

/**
 * <pre>
 * HEADER
 * --------
 * LEVEL (2 BYTES)
 * ENTRIES (4 BYTES)
 * BLOCK_SIZE (2 BYTES)
 *
 * LEAF NODE
 * ---------
 * KEY [N bytes]
 * LOG_POS (8 BYTES)
 *
 * ==============
 *
 * INTERNAL NODE
 * ---------
 * STREAM (8 BYTES)
 * VERSION (4 BYTES)
 * BLOCK_IDX (4 BYTES)
 * </pre>
 */
class Block {


    protected final ByteBuffer data;
    private final RowKey rowKey;
    private int tmpEntries;
    private static final int LEVEL_OFFSET = 0;
    private static final int ENTRIES_OFFSET = LEVEL_OFFSET + Short.BYTES;
    private static final int BLOCK_SIZE = ENTRIES_OFFSET + Integer.BYTES;

    //common for both leaf and internal nodes
    static final int HEADER = Short.BYTES + Integer.BYTES + Short.BYTES;


    Block(RowKey rowKey, int size, int level) {
        this.rowKey = rowKey;
        assert size <= Short.MAX_VALUE : "Block must not exceed " + Short.MAX_VALUE;
        this.data = Buffers.allocate(size, false);
        data.putShort((short) level);
        data.position(HEADER);
    }

    private Block(RowKey rowKey, ByteBuffer buffer) {
        this.rowKey = rowKey;
        this.data = buffer;
    }

    static Block from(RowKey rowKey, ByteBuffer buffer) {
        return new Block(rowKey, buffer);
    }

    boolean add(ByteBuffer rec, long logPos) {
        assert level() == 0 : "Not a leaf node";
        if (data.remaining() < leafEntrySize(rowKey)) {
            return false;
        }
        Record.copyKey(rec, data);
        data.putLong(logPos);

        tmpEntries++;

        return true;
    }

    boolean addLink(Block ref, int idx) {
        assert level() != 0 : "Not an internal node";
        if (data.remaining() < internalEntrySize(rowKey)) {
            return false;
        }

        tmpEntries += ref.entries();

        data.putLong(ref.firstStream());
        data.putInt(ref.firstVersion());
        data.putInt(idx);
        return true;
    }

    int writeTo(MappedRegion mf) {
        assert mf.remaining() >= data.capacity() : "Not enough index space";

        data.putInt(ENTRIES_OFFSET, tmpEntries);
        data.putShort(BLOCK_SIZE, (short) data.position());

        data.clear();
        int position = mf.position();
        mf.put(data);
        data.clear().position(HEADER);
        tmpEntries = 0;
        return position / data.capacity();
    }

    int level() {
        return data.getShort(LEVEL_OFFSET);
    }

    int actualSize() {
        return data.getShort(BLOCK_SIZE);
    }

    int dataSize() {
        return actualSize() - HEADER;
    }

    int entries() {
        return data.getInt(ENTRIES_OFFSET);
    }

    long firstStream() {
        return data.getLong(HEADER);
    }

    int firstVersion() {
        return data.getInt(HEADER + Long.BYTES);
    }

    int blockEntries() {
        int level = level();
        if (level == 0) { //leaf
            return entries();
        }
        //internal
        return (actualSize() - HEADER) / internalEntrySize(rowKey);
    }

    boolean hasData() {
        return data.position() > HEADER;
    }

    int find(ByteBuffer key, IndexFunction fn) {
        int dataSize = dataSize();
        int entrySize = level() == 0 ? leafEntrySize(rowKey) : internalEntrySize(rowKey);
        int idx = binarySearch(HEADER, dataSize, entrySize, key);
        return fn.apply(idx);
    }

    //internal only
    int blockIndex(int idx) {
        int pos = offset(idx);
        return data.getInt(pos + rowKey.keySize());
    }

    //leaf only
    long logPos(int idx) {
        int pos = offset(idx);
        return data.getLong(pos + rowKey.keySize());
    }

    private int offset(int idx) {
        int entrySize = level() == 0 ? leafEntrySize(rowKey) : internalEntrySize(rowKey);
        return HEADER + (idx * entrySize);
    }


    private int binarySearch(int chunkStart, int chunkLength, int entrySize, ByteBuffer key) {
        int low = 0;
        int high = blockEntries() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int dataOffset = chunkStart + (mid * entrySize);
            assert dataOffset >= chunkStart && dataOffset <= chunkStart + chunkLength : "Index out of bounds: " + dataOffset;
            int cmp = compare(data, dataOffset, key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    private int compare(ByteBuffer buffer, int bufferPos, ByteBuffer key) {
        return rowKey.compare(data, bufferPos, key, key.position());
    }

    static int leafEntrySize(RowKey rowKey) {
        return rowKey.keySize() + Long.BYTES;
    }

    static int internalEntrySize(RowKey rowKey) {
        return rowKey.keySize() + Long.BYTES;
    }

    @Override
    public String toString() {
        return "level=" + level() +
                ", entries= " + entries() +
                ", blockEntries= " + blockEntries() +
                ", actualSize= " + actualSize() +
                " [" + firstStream() + "@" + firstVersion() + "]";
    }
}
