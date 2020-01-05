package io.joshworks.ilog;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * A UNIQUE, ORDERED entry index
 * - All entries must have the same size
 * - Insertion must be in ORDERED fashion
 * - All entries must be unique
 * <p>
 * If opening from an existing file, the index is marked as read only.
 */
public abstract class Index implements TreeFunctions, Closeable {

    protected final MappedFile mf;
    private final int maxEntries;
    private final int keySize;
    private int entries;
    private final AtomicBoolean readOnly = new AtomicBoolean();

    public static final int NONE = -1;

    public Index(File file, int size, int keySize) {
        this.keySize = keySize;
        try {
            boolean newFile = file.createNewFile();
            if (newFile) {
                int alignedSize = align(size);
                this.maxEntries = alignedSize / entrySize();
                this.mf = MappedFile.create(file, alignedSize);
            } else { //existing file
                //empty buffer, no writes wil be allowed anyways
                this.mf = MappedFile.open(file);
                long fileSize = mf.capacity();
                if (fileSize % entrySize() != 0) {
                    throw new IllegalStateException("Invalid index file length: " + fileSize);
                }
                this.maxEntries = (int) (fileSize / entrySize());
                this.entries = (int) (fileSize / entrySize());
                readOnly.set(true);
            }

        } catch (IOException ioex) {
            throw new RuntimeException("Failed to create index", ioex);
        }
    }

    protected abstract int compare(ByteBuffer k1, int pos);

    public void write(Record record, long position) {
        if (readOnly.get()) {
            throw new RuntimeException("Index is read only");
        }
        record.writeKey(mf);
        mf.putLong(position);
        entries++;
    }

    /**
     * Complete this index and mark it as read only.
     */
    public void complete() {
        mf.flush();
        mf.truncate(mf.position());
        readOnly.set(true);
    }

    /**
     * Returns the start slot position that the key is contained, null if the key is less than the first item
     */
    public long floor(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }
        int idx = binarySearch(key);
        idx = idx >= 0 ? idx : Math.abs(idx) - 2;
        return readPosition(idx);
    }

    @Override
    public long ceiling(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }

        int idx = binarySearch(key);
        idx = idx >= 0 ? idx : Math.abs(idx) - 1;
        return readPosition(idx);
    }

    @Override
    public long higher(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }

        int idx = binarySearch(key);
        idx = idx >= 0 ? idx + 1 : Math.abs(idx) - 1;
        return readPosition(idx);
    }

    @Override
    public long lower(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }
        int idx = binarySearch(key);
        idx = idx > 0 ? idx - 1 : Math.abs(idx) - 2;
        return readPosition(idx);
    }

    @Override
    public long get(ByteBuffer key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }
        int idx = binarySearch(key);
        if (idx < 0) {
            return NONE;
        }
        return readPosition(idx);
    }

    private int binarySearch(ByteBuffer key) {
        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = compareTo(key, mid);
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
        return entries >= maxEntries;
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

    @Override
    public void close() {
        mf.close();
    }

    private int entrySize() {
        return keySize + Long.BYTES;
    }

    public int keySize() {
        return keySize;
    }

    private long readPosition(int idx) {
        if (idx < 0 || idx >= entries) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + keySize;
        return mf.getLong(positionOffset);
    }

    private int compareTo(ByteBuffer key, int idx) {
        if (idx < 0 || idx >= entries) {
            throw new IllegalStateException("Index must be between 0 and " + entries + ", got " + idx);
        }
        int startPos = idx * entrySize();
        return compare(key, startPos);
    }

    private int align(int size) {
        int entrySize = entrySize();
        return entrySize * (size / entrySize);
    }

    public String name() {
        return mf.name();
    }
}
