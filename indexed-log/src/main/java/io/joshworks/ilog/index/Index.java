package io.joshworks.ilog.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.record.Record;

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
    private final RowKey rowKey;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    public static final int NONE = -1;

    public Index(File file, long maxEntries, RowKey rowKey) {
        this.rowKey = rowKey;
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
     */
    public void write(Record rec, long recordPos) {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }
        if (rec.keyLen() != keySize()) {
            throw new RuntimeException("Invalid index key length, expected " + keySize() + ", got " + rec.keyLen());
        }
        int pos = mf.position();

        rec.copyKey(mf.buffer());
        mf.putLong(recordPos);
        mf.putInt(rec.recordSize());

        assert entrySize() == (mf.position() - pos);
    }

    public int get(ByteBuffer key) {
        return find(key, IndexFunction.EQUALS);
    }

    public int floor(ByteBuffer key) {
        return find(key, IndexFunction.FLOOR);
    }

    public int ceiling(ByteBuffer key) {
        return find(key, IndexFunction.CEILING);
    }

    public int lower(ByteBuffer key) {
        return find(key, IndexFunction.LOWER);
    }

    public int higher(ByteBuffer key) {
        return find(key, IndexFunction.HIGHER);
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
        return ByteBufferBinarySearch.binarySearch(key, mf.buffer(), 0, size(), entrySize(), rowKey);
    }

    public long readPosition(int idx) {
        if (idx < 0 || idx >= entries()) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + keySize();
        return mf.getLong(positionOffset);
    }

    public int readEntrySize(int idx) {
        if (idx < 0 || idx >= entries()) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + keySize() + Long.BYTES;
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
        return keySize() + Long.BYTES + Integer.BYTES;
    }

    public int keySize() {
        return rowKey.keySize();
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