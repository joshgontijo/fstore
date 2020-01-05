package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;

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
public class Index<K extends Comparable<K>> implements TreeFunctions<K>, Closeable {

    private final MappedFile mf;
    private final KeyParser<K> parser;
    private final int maxEntries;
    private int entries;
    private final ByteBuffer writeBuffer;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private K last; //only used to ensure ordering

    public static final int NONE = -1;

    public Index(File file, int maxSize, KeyParser<K> parser) {
        this.parser = parser;
        try {
            boolean newFile = file.createNewFile();
            if (newFile) {
                int alignedSize = align(maxSize);
                this.maxEntries = alignedSize / entrySize();
                this.writeBuffer = Buffers.allocate(entrySize(), false);
                this.mf = MappedFile.create(file, alignedSize);
            } else { //existing file
                //empty buffer, no writes wil be allowed anyways
                this.mf = MappedFile.open(file);
                long fileSize = mf.size();
                if (fileSize % entrySize() != 0) {
                    throw new IllegalStateException("Invalid index file length: " + fileSize);
                }
                this.maxEntries = (int) (fileSize / entrySize());
                this.entries = (int) (fileSize / entrySize());
                this.writeBuffer = null;
                readOnly.set(true);
            }

        } catch (IOException ioex) {
            throw new RuntimeException("Failed to create index", ioex);
        }
    }

//    public void write(ByteBuffer key, long position) {
//        write(parser.readFrom(key), position);
//        writeInternal(key, position);
//    }

    public void write(Record record, long position) {
        record.writeKey(mf);
        mf.putLong(position);
        entries++;
    }

    public void write(K key, long position) {
        if (readOnly.get()) {
            throw new RuntimeException("Index is read only");
        }
        validateEntry(key);
        last = key;
        writeInternal(key, position);
    }

    private void validateEntry(K key) {
        requireNonNull(key, "Key must not be null");
        if (last == null) {
            return;
        }
        int compare = last.compareTo(key);
        if (last.compareTo(key) > 0) {
            throw new IllegalArgumentException("Index entries must be ordered. Entry " + key + " must be greater than previous entry " + last);
        }
        if (compare == 0) {
            throw new IllegalArgumentException("Duplicate index entry " + key);
        }
    }

    private void writeInternal(K key, long position) {
        parser.writeTo(key, writeBuffer);
        writeBuffer.putLong(position);
        writeBuffer.flip();
        mf.write(writeBuffer);
        writeBuffer.clear();
        entries++;
    }

    /**
     * Complete this index and mark it as read only.
     */
    public void complete() {
        readOnly.set(true);
//        mbb.truncate(mbb.length());
    }

    public void flush() {
//        mbb.flush(false);
    }

    public IndexEntry<K> find(K key) {
        throw new UnsupportedOperationException("TODO");
    }

    /**
     * Returns the start slot position that the key is contained, null if the key is less than the first item
     */
    public long floor(K key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }
//        if (key.compareTo(first.key) < 0) {
//            return null; //less than first entry
//        }
        int idx = binarySearch(key);
        idx = idx >= 0 ? idx : Math.abs(idx) - 2;
        return readPosition(idx);
    }

    @Override
    public long ceiling(K key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }
//        if (key.compareTo(last.key) > 0) {
//            return null; //less or equals than first entry
//        }

        int idx = binarySearch(key);
        idx = idx >= 0 ? idx : Math.abs(idx) - 1;
        return readPosition(idx);
    }

    @Override
    public long higher(K key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }
//        if (key.compareTo(last.key) >= 0) {
//            return null; //less or equals than first entry
//        }

        int idx = binarySearch(key);
        idx = idx >= 0 ? idx + 1 : Math.abs(idx) - 1;
        return readPosition(idx);
    }

    @Override
    public long lower(K key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return NONE;
        }
//        if (key.compareTo(first.key) <= 0) {
//            return null; //less or equals than first entry
//        }

        int idx = binarySearch(key);
        idx = idx > 0 ? idx - 1 : Math.abs(idx) - 2;
        return readPosition(idx);
    }

    @Override
    public long get(K key) {
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

    public K first() {
        if (entries == 0) {
            return null;
        }
        return readKey(0);
    }

    public K last() {
        if (entries == 0) {
            return null;
        }
        return readKey(entries - 1);
    }

    private int binarySearch(K key) {
        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            K midKey = readKey(mid);
            int cmp = midKey.compareTo(key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found, exact match
        }
        return -(low + 1);
    }

    public boolean isFull() {
        return entries == maxEntries;
    }

    public long entries() {
        return entries;
    }

    public void delete() throws IOException {
        mf.delete();
    }

    public void truncate(long position) {
        mf.truncate(position);
    }

    @Override
    public void close() {
        mf.close();
    }

    private int entrySize() {
        return parser.keySize() + Long.BYTES;
    }

    private long readPosition(int idx) {
        if (idx < 0 || idx >= entries) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + parser.keySize();
        BufferReader reader = mf.reader(positionOffset);
        return reader.getLong();
    }

    private K readKey(int idx) {
        if (idx < 0 || idx >= entries) {
            throw new IllegalStateException("Index must be between 0 and " + entries + ", got " + idx);
        }
        int startPos = idx * entrySize();
        BufferReader reader = mf.reader(startPos);
        return parser.readFrom(reader);
    }

    private int align(int size) {
        int entrySize = entrySize();
        return entrySize * (size / entrySize);
    }

    public int keySize() {
        return parser.keySize();
    }
}
