package io.joshworks.ilog.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.record.Record2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

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
public class Index implements Closeable {

    private final MappedFile mf;
    private final RowKey comparator;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    public static final int NONE = -1;

    public static int MAX_SIZE = Integer.MAX_VALUE - 8;

    public Index(File file, long maxEntries, RowKey comparator) {
        this.comparator = comparator;
        try {
            boolean newFile = file.createNewFile();
            if (newFile) {
                long alignedSize = align(entrySize() * maxEntries);
                this.mf = MappedFile.create(file, alignedSize);
            } else { //existing file
                this.mf = MappedFile.open(file);
                long fileSize = mf.capacity();
                if (fileSize % entrySize() != 0) {
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
     *
     * @param src      The source buffer to get the key from
     * @param kOffset  The key offset in the source buffer
     * @param kCount   The size of the key, must match Rowkey#keySize()
     * @param position The entry position in the log
     */
    public void write(Record2 rec, long recordPos) {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }
        if (rec.keySize() != comparator.keySize()) {
            throw new RuntimeException("Invalid index key length, expected " + comparator.keySize() + ", got " + rec.keySize());
        }
        rec.copyKey(mf.buffer());
        mf.putLong(recordPos);
        mf.putInt(rec.recordSize());
    }

    public int find(ByteBuffer key, IndexFunction func) {
        requireNonNull(key, "Key must be provided");
        int remaining = key.remaining();
        int kSize = keySize();
        if (remaining != kSize) {
            throw new IllegalArgumentException("Invalid key size: " + remaining + ", expected: " + kSize);
        }
        if (entries() == 0) {
            return NONE;
        }
        int idx = binarySearch(key);
        return func.apply(idx);
    }


    private int binarySearch(ByteBuffer key) {
        return ByteBufferBinarySearch.binarySearch(key, mf.buffer(), 0, size(), entrySize(), comparator);
    }

    public long readPosition(int idx) {
        if (idx < 0 || idx >= entries()) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + comparator.keySize();
        return mf.getLong(positionOffset);
    }

    public int readEntrySize(int idx) {
        if (idx < 0 || idx >= entries()) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + comparator.keySize() + Long.BYTES;
        return mf.getInt(positionOffset);
    }

    public boolean isFull() {
        return mf.position() >= mf.capacity();
    }

    public int entries() {
        //there can be a partial write in the buffer, doing this makes sure it won't be considered
        int entrySize = entrySize();
        return (mf.position() / entrySize);
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

    protected int entrySize() {
        return comparator.keySize() + Long.BYTES + Integer.BYTES;
    }

    public int keySize() {
        return comparator.keySize();
    }

    private long align(long size) {
        int entrySize = entrySize();
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
        return entries() * entrySize();
    }

    public int capacity() {
        return mf.capacity();
    }

    public void first(ByteBuffer dst) {
        if (entries() == 0) {
            return;
        }
        if (dst.remaining() != keySize()) {
            throw new RuntimeException("Buffer key length mismatch");
        }
        mf.get(dst, 0, keySize());
    }

    public int remaining() {
        return mf.capacity() / entrySize();
    }

}