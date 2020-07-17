package io.joshworks.es.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.mmap.MappedFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A NON-CLUSTERED, UNIQUE, ORDERED index that uses binary search to read elements
 * - Append only
 * - Entries must be of a fixed size
 * - Insertion must be ORDERED
 * - Entries must be unique
 * <p>
 * If opening from an existing file, the index is marked as read only.
 * <p>
 * FORMAT:
 * KEY (N bytes)
 * LOG_POS (8 bytes)
 */
public class IndexSegment implements Closeable {

    private static final int ENTRY_SIZE =
            Long.BYTES + //stream
                    Integer.BYTES + //version
                    Integer.BYTES + //size
                    Long.BYTES; // logPos

    private static int STREAM_OFFSET = 0;
    private static int VERSION_OFFSET = STREAM_OFFSET + Long.BYTES;
    private static int SIZE_OFFSET = VERSION_OFFSET + Integer.BYTES;
    private static int LOGPOS_OFFSET = SIZE_OFFSET + Integer.BYTES;

    private final MappedFile mf;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    public static final int NONE = -1;

    public IndexSegment(File file, long maxEntries) {
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
        mf.putLong(stream);
        mf.putInt(version);
        mf.putInt(size);
        mf.putLong(logPos);
    }

    public int find(long stream, int version, IndexFunction func) {
        if (entries() == 0) {
            return NONE;
        }
        //TODO add midpoints and start b-search from chunks
        int idx = binarySearch(0, mf.position(), stream, version);
        return func.apply(idx);
    }

    public int binarySearch(int chunkStart, int chunkLength, long stream, int version) {
        int low = 0;
        int high = entries() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int readPos = chunkStart + (mid * ENTRY_SIZE);
            if (readPos < chunkStart || readPos > chunkStart + chunkLength) {
                throw new IndexOutOfBoundsException("Index out of bounds: " + readPos);
            }
            int cmp = compare(readPos, stream, version);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    private int compare(int bufferPos, long stream, int version) {
        MappedByteBuffer buffer = mf.buffer();
        long bstream = buffer.getLong(bufferPos + STREAM_OFFSET);
        int bversion = buffer.getInt(bufferPos + VERSION_OFFSET);
        int compare = Long.compare(bstream, stream);
        if (compare != 0) {
            return compare;
        }
        return Integer.compare(bversion, version);
    }

    public int version(int idx) {
        checkBounds(idx);
        return mf.getInt(idx * ENTRY_SIZE + VERSION_OFFSET);
    }

    public long stream(int idx) {
        checkBounds(idx);
        return mf.getLong(idx * ENTRY_SIZE + STREAM_OFFSET);
    }

    public int size(int idx) {
        checkBounds(idx);
        return mf.getInt(idx * ENTRY_SIZE + SIZE_OFFSET);
    }

    public long logPos(int idx) {
        checkBounds(idx);
        return mf.getLong(idx * ENTRY_SIZE + LOGPOS_OFFSET);
    }

    private void checkBounds(int idx) {
        if (idx < 0 || idx >= entries()) {
            throw new IndexOutOfBoundsException(idx);
        }
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

}