package io.joshworks.ilog.index;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.lsm.BufferBinarySearch;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * A NON-CLUSTERED, UNIQUE, ORDERED index
 * - Index entries must be of a fixed size
 * - Insertion must be ORDERED
 * - All entries must be unique
 * <p>
 * If opening from an existing file, the index is marked as read only.
 * <p>
 * FORMAT:
 * KEY (N bytes)
 * LOG_POS (8 bytes)
 */
public class Index implements Closeable {

    private final MappedFile mf;
    private final KeyComparator comparator;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    public static final int NONE = -1;

    public static int MAX_SIZE = Integer.MAX_VALUE - 8;

    public Index(File file, int size, KeyComparator comparator) {
        this.comparator = comparator;
        try {
            boolean newFile = file.createNewFile();
            if (newFile) {
                int alignedSize = align(size);
                this.mf = MappedFile.create(file, alignedSize);
            } else { //existing file
                //empty buffer, no writes wil be allowed anyways
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

    public void write(ByteBuffer record, long position) {
        if (readOnly.get()) {
            throw new RuntimeException("Index is read only");
        }

        int keySize = Record2.keySize(record);
        if (keySize != comparator.keySize()) {
            throw new RuntimeException("Invalid index key length, expected " + comparator.keySize() + ", got " + keySize);
        }

        MappedByteBuffer dst = mf.buffer();
        if (dst.remaining() < keySize) {
            throw new IllegalStateException("Not enough index space");
        }
        doWrite(keySize, position, record, dst);
    }

    public int find(ByteBuffer key, IndexFunctions func) {
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

    private void doWrite(int keySize, long position, ByteBuffer record, ByteBuffer dst) {
        int rsize = Record2.sizeOf(record);
        int written = Record2.writeKey(record, dst);
        if (written != keySize) {
            Buffers.offsetPosition(dst, -written);
            throw new IllegalStateException("Expected " + keySize + " bytes written to index, actual: " + written);
        }
        dst.putLong(position);
        dst.putInt(rsize);
    }

    private int binarySearch(ByteBuffer key) {
        return BufferBinarySearch.binarySearch(key, mf.buffer(), 0, size(), entrySize(), comparator);
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

    public void delete() throws IOException {
        mf.delete();
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


    private int align(int size) {
        int entrySize = entrySize();
        return entrySize * (size / entrySize);
    }

    public String name() {
        return mf.name();
    }

    public int size() {
        return entries() * entrySize();
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
}