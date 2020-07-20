package io.joshworks.es.index;

import io.joshworks.es.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.mmap.MappedFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.joshworks.es.index.Index.NONE;


public class BTreeIndexSegment implements SegmentFile {

    private static final int ENTRY_SIZE =
            Long.BYTES + //stream
                    Integer.BYTES + //version
                    Integer.BYTES + //size
                    Long.BYTES; // logPos


    private final MappedFile mf;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final int blockSize;

    private ByteBuffer writeBuffer;

    public BTreeIndexSegment(File file, long maxEntries, int blockSize) {
        this.blockSize = blockSize;
        this.writeBuffer = Buffers.allocate(blockSize, false);
        try {
            boolean newFile = file.createNewFile();
            if (newFile) {
                long alignedSize = align(ENTRY_SIZE * maxEntries);
                this.mf = MappedFile.create(file, alignedSize);
            } else { //existing file
                this.mf = MappedFile.open(file);
                long fileSize = mf.capacity();
                if (fileSize % ENTRY_SIZE != 0) {
                    throw new IllegalStateException("Invalid index file length: " + fileSize);
                }
                readOnly.set(true);
            }

        } catch (IOException ioex) {
            throw new RuntimeException("Failed to create index", ioex);
        }
    }


    /**
     * Writes an entry to this index
     */
    public void append(long stream, int version, int size, long logPos) {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }
        addEntry(stream, version, size, logPos);
    }

    private boolean addEntry(long stream, int version, int size, long logPos) {
        if(writeBuffer.remaining() < )
        writeBuffer.putLong(stream);
        writeBuffer.putInt(version);
        writeBuffer.putInt(size);
        writeBuffer.putLong(logPos);
    }

    public IndexEntry find(IndexKey key, IndexFunction func) {
        if (entries() == 0) {
            return null;
        }
        //TODO add midpoints and start b-search from chunks
        int idx = binarySearch(0, mf.position(), key);
        idx = func.apply(idx);
        if (idx == NONE) {
            return null;
        }

        long stream = mf.getLong(idx * ENTRY_SIZE + STREAM_OFFSET);
        int version = mf.getInt(idx * ENTRY_SIZE + VERSION_OFFSET);
        int size = mf.getInt(idx * ENTRY_SIZE + SIZE_OFFSET);
        long logAddress = mf.getLong(idx * ENTRY_SIZE + LOGPOS_OFFSET);

        return new IndexEntry(stream, version, size, logAddress);
    }

    public int binarySearch(int chunkStart, int chunkLength, IndexKey key) {
        int low = 0;
        int high = entries() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int readPos = chunkStart + (mid * ENTRY_SIZE);
            if (readPos < chunkStart || readPos > chunkStart + chunkLength) {
                throw new IndexOutOfBoundsException("Index out of bounds: " + readPos);
            }
            int cmp = compare(readPos, key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    private int compare(int bufferPos, IndexKey key) {
        MappedByteBuffer buffer = mf.buffer();
        long bstream = buffer.getLong(bufferPos + STREAM_OFFSET);
        int bversion = buffer.getInt(bufferPos + VERSION_OFFSET);

        return IndexKey.compare(key, bstream, bversion) * -1;
    }


    public boolean isFull() {
        return mf.position() >= mf.capacity();
    }

    public int entries() {
        //there can be a partial write in the buffer, doing this makes sure it won't be considered
        return (mf.position() / ENTRY_SIZE);
    }

    public void delete() {
        try {
            mf.delete();
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to delete index");
        }
    }

    @Override
    public File file() {
        return mf.file();
    }

    public void truncate() {
        mf.truncate(mf.position());
    }

    /**
     * Complete this index and mark it as read only.
     */
    public void complete() {
        truncate();
        readOnly.set(true);
    }

    public void flush() {
        mf.flush();
    }

    @Override
    public void close() {
        mf.close();
    }


    private long align(long size) {
        int entrySize = ENTRY_SIZE;
        long aligned = entrySize * (size / entrySize);
        if (aligned <= 0) {
            throw new IllegalArgumentException("Invalid index size: " + size);
        }
        return aligned;
    }

    public String name() {
        return mf.name();
    }

    public int size() {
        return entries() * ENTRY_SIZE;
    }

    /**
     * <pre>
     * STREAM (8 BYTES)
     * VERSION (4 BYTES)
     * SIZE (4 BYTES)
     * LOG_POS (8 BYTES)
     * </pre>
     */
    private static class LeafNode {

    }

    /**
     * <pre>
     * STREAM (8 BYTES)
     * VERSION (4 BYTES)
     * BLOCK_IDX (4 BYTES)
     * </pre>
     */
    private static class InternalNode {

    }

    //TODO implement ?
    /**
     * <pre>
     * STREAM (8 BYTES)
     * COUNT (4 BYTES)
     * START_VERSION (4 BYTES)
     * [
     *   SIZE (4 BYTES)
     *   LOG_POS (8 BYTES)
     * ]
     * </pre>
     */
    private static class LeafNodeCompact {

    }



}