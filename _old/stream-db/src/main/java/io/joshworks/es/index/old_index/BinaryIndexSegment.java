package io.joshworks.es.index.old_index;

import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.index.IndexSegment;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.mmap.MappedFile;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.joshworks.es.index.Index.NONE;

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
@Deprecated
public class BinaryIndexSegment implements IndexSegment {

    private static final int ENTRY_SIZE =
            Long.BYTES + //stream
                    Integer.BYTES + //version
                    Long.BYTES; // logPos

    private static int STREAM_OFFSET = 0;
    private static int VERSION_OFFSET = STREAM_OFFSET + Long.BYTES;
    private static int LOGPOS_OFFSET = VERSION_OFFSET + Integer.BYTES;

    private final MappedFile mf;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final int midpointFactor;

    private final List<IndexEntry> midpoints = new ArrayList<>();

    public BinaryIndexSegment(File file, long maxEntries, int midpointFactor) {
        this.midpointFactor = midpointFactor;
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
                loadMidpoints();
            }

        } catch (IOException ioex) {
            throw new RuntimeException("Failed to create index", ioex);
        }
    }

    private void loadMidpoints() {
        midpoints.clear();
        //TODO
    }

    /**
     * Writes an entry to this index
     */
    @Override
    public void append(long stream, int version, long logPos) {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }
        mf.putLong(stream);
        mf.putInt(version);
        mf.putLong(logPos);
    }

    @Override
    public IndexEntry find(long stream, int version, IndexFunction func) {
        if (entries() == 0) {
            return null;
        }
        //TODO add midpoints and start b-search from chunks
        int idx = binarySearch(0, mf.position(), new IndexKey(stream, version));
        idx = func.apply(idx);
        if (idx == NONE) {
            return null;
        }

        long foundStream = mf.getLong(idx * ENTRY_SIZE + STREAM_OFFSET);
        int foundVersion = mf.getInt(idx * ENTRY_SIZE + VERSION_OFFSET);
        long logAddress = mf.getLong(idx * ENTRY_SIZE + LOGPOS_OFFSET);

        return new IndexEntry(foundStream, foundVersion, logAddress);
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


    @Override
    public boolean isFull() {
        return mf.position() >= mf.capacity();
    }

    @Override
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

    @Override
    public void truncate() {
        mf.truncate(mf.position());
    }

    /**
     * Complete this index and mark it as read only.
     */
    @Override
    public void complete() {
        mf.flush();
        truncate();
        readOnly.set(true);
        loadMidpoints();
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

    @Override
    public String name() {
        return mf.name();
    }

    @Override
    public int size() {
        return entries() * ENTRY_SIZE;
    }

}