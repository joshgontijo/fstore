package io.joshworks.es.index.btree;

import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexKey;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.mmap.MappedRegion;

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
 * STREAM (8 BYTES)
 * VERSION (4 BYTES)
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


    static final int KEY_BYTES =
                Long.BYTES + //STREAM
                    Integer.BYTES; //VERSION

    static final int LEAF_ENTRY_BYTES =
                    KEY_BYTES +
                    Long.BYTES; // LOG_POS


    static final int INTERNAL_ENTRY_BYTES =
                    KEY_BYTES +
                    Integer.BYTES; //BLOCK_IDX


    protected final ByteBuffer data;
    private int tmpEntries;
    private static final int LEVEL_OFFSET = 0;
    private static final int ENTRIES_OFFSET = LEVEL_OFFSET + Short.BYTES;
    private static final int BLOCK_SIZE = ENTRIES_OFFSET + Integer.BYTES;

    //common for both leaf and internal nodes
    static final int HEADER = Short.BYTES + Integer.BYTES + Short.BYTES;


    Block(int size, int level) {
        assert size <= Short.MAX_VALUE : "Block must not exceed " + Short.MAX_VALUE;
        this.data = Buffers.allocate(size, false);
        data.putShort((short) level);
        data.position(HEADER);
    }

    private Block(ByteBuffer buffer) {
        this.data = buffer;
    }

    static Block from(ByteBuffer buffer) {
        return new Block(buffer);
    }

    boolean add(long stream, int version, long logPos) {
        assert level() == 0 : "Not a leaf node";
        if (data.remaining() < LEAF_ENTRY_BYTES) {
            return false;
        }
        data.putLong(stream);
        data.putInt(version);
        data.putLong(logPos);

        tmpEntries++;

        return true;
    }

    boolean addLink(Block ref, int idx) {
        assert level() != 0 : "Not an internal node";
        if (data.remaining() < INTERNAL_ENTRY_BYTES) {
            return false;
        }

        tmpEntries += ref.entries();

        data.putLong(ref.firstStream());
        data.putInt(ref.firstVersion());
        data.putInt(idx);
        return true;
    }

    int writeTo(MappedRegion mf) {
        assert mf.remaining() >= data.capacity()  : "Not enough index space";

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
        return (actualSize() - HEADER) / INTERNAL_ENTRY_BYTES;
    }

    boolean hasData() {
        return data.position() > HEADER;
    }

    int find(long stream, int version, IndexFunction fn) {
        int dataSize = dataSize();
        int entrySize = level() == 0 ? LEAF_ENTRY_BYTES : INTERNAL_ENTRY_BYTES;
        int idx = binarySearch(HEADER, dataSize, entrySize, stream, version);
        return fn.apply(idx);
    }

    IndexEntry toIndexEntry(int idx) {
        long stream = stream(idx);
        int version = version(idx);
        long logPos = logPos(idx);
        return new IndexEntry(stream, version, logPos);
    }

    //entry key
    long stream(int idx) {
        int pos = offset(idx);
        return data.getLong(pos);
    }

    //entry key
    int version(int idx) {
        int pos = offset(idx);
        return data.getInt(pos + Long.BYTES);
    }

    //internal only
    int blockIndex(int idx) {
        int pos = offset(idx);
        return data.getInt(pos + KEY_BYTES);
    }

    //leaf only
    int recordSize(int idx) {
        int pos = offset(idx);
        return data.getInt(pos + KEY_BYTES);
    }

    //leaf only
    long logPos(int idx) {
        int pos = offset(idx);
        return data.getLong(pos + KEY_BYTES);
    }

    private int offset(int idx) {
        int entrySize = level() == 0 ? LEAF_ENTRY_BYTES : INTERNAL_ENTRY_BYTES;
        return HEADER + (idx * entrySize);
    }


    private int binarySearch(int chunkStart, int chunkLength, int entrySize, long stream, int version) {
        int low = 0;
        int high = blockEntries() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int readPos = chunkStart + (mid * entrySize);
            assert readPos >= chunkStart && readPos <= chunkStart + chunkLength : "Index out of bounds: " + readPos;
            int cmp = compare(readPos, data, stream, version);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    private int compare(int bufferPos, ByteBuffer buffer, long keyStream, int keyVersion) {
        long stream = buffer.getLong(bufferPos);
        int version = buffer.getInt(bufferPos + Long.BYTES);

        return IndexKey.compare(stream, version, keyStream, keyVersion);
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
