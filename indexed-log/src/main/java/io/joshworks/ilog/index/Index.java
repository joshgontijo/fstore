package io.joshworks.ilog.index;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record2;

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
    private int entries;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final BufferPool pool;
    public static final int NONE = -1;

    public static int MAX_SIZE = Integer.MAX_VALUE - 8;

    public Index(File file, int size, KeyComparator comparator) {
        this.comparator = comparator;
        this.pool = BufferPool.localCachePool(1, comparator.keySize(), false);
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
                this.entries = (int) (fileSize / entrySize());
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
        entries++;
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

    public int get(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        int remaining = key.remaining();
        int kSize = keySize();
        if (remaining != kSize) {
            throw new IllegalArgumentException("Invalid key size: " + remaining + ", expected: " + kSize);
        }
        if (entries == 0) {
            return NONE;
        }
        var bb = pool.allocate();
        try {
            int idx = binarySearch(key, bb);
            return Math.max(idx, NONE);
        } finally {
            pool.free(bb);
        }
    }

    /**
     * Returns the start slot position that the key is contained, null if the key is less than the first item
     */
    public int floor(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }

        var bb = pool.allocate();
        try {
            int idx = binarySearch(key, bb);
            return idx >= 0 ? idx : Math.abs(idx) - 2;
        } finally {
            pool.free(bb);
        }
    }

    public int ceiling(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }

        var bb = pool.allocate();
        try {
            int idx = binarySearch(key, bb);
            return idx >= 0 ? idx : Math.abs(idx) - 1;
        } finally {
            pool.free(bb);
        }
    }

    public int higher(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }

        var bb = pool.allocate();
        try {
            int idx = binarySearch(key, bb);
            return idx >= 0 ? idx + 1 : Math.abs(idx) - 1;
        } finally {
            pool.free(bb);
        }
    }

    public int lower(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }
        var bb = pool.allocate();
        try {
            int idx = binarySearch(key, bb);
            return idx > 0 ? idx - 1 : Math.abs(idx) - 2;
        } finally {
            pool.free(bb);
        }
    }

    public long readPosition(int idx) {
        if (idx < 0 || idx >= entries) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + comparator.keySize();
        return mf.getLong(positionOffset);
    }

    public int readEntrySize(int idx) {
        if (idx < 0 || idx >= entries) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + comparator.keySize() + Long.BYTES;
        return mf.getInt(positionOffset);
    }

    private int binarySearch(ByteBuffer key, ByteBuffer read) {
        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = compareTo(key, mid, read);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    public boolean isFull() {
        return mf.position() >= mf.capacity();
    }

    public long entries() {
        return entries;
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

    /**
     * Function to compare a given key k1 to a value present in the index map at position pos
     * The function must read the key from the MappedByteBuffer without modifying its position
     * Therefore it must always use the ABSOLUTE getXXX methods from the buffer.
     */
    private int compare(int idx, ByteBuffer key, ByteBuffer read) {
        mf.get(read, idx, comparator.keySize());
        read.flip();

        int prevPos = key.position();
        int prevLimit = key.limit();

        int compare = comparator.compare(read, key);

        key.limit(prevLimit);
        key.position(prevPos);

        read.clear();
        return compare;
    }

    private int compareTo(ByteBuffer key, int idx, ByteBuffer read) {
        if (idx < 0 || idx >= entries) {
            throw new IllegalStateException("Index must be between 0 and " + entries + ", got " + idx);
        }
        int startPos = idx * entrySize();

        //mark
        int pos = key.position();
        int limit = key.limit();
        int cmp = compare(startPos, key, read);
        //reset
        key.limit(limit).position(pos);
        return cmp;

    }

    private int align(int size) {
        int entrySize = entrySize();
        return entrySize * (size / entrySize);
    }

    public String name() {
        return mf.name();
    }

    public int size() {
        return mf.capacity();
    }

    public void first(ByteBuffer dst) {
        if (entries == 0) {
            return;
        }
        if (dst.remaining() != keySize()) {
            throw new RuntimeException("Buffer key length mismatch");
        }
        mf.get(dst, 0, keySize());
    }
}