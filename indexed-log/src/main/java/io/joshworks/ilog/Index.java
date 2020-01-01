package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

//File does not support reopening, must be created from scratch every time a segment is created

/**
 * A UNIQUE, ORDERED entry index
 * - All entries must have the same size
 * - Insertion must be in ORDERED fashion
 * - All entries must be unique
 * <p>
 * If opening from an existing file, the index is marked as read only.
 */
public class Index<K extends Comparable<K>> {

    protected final Storage storage;
    protected final Serializer<K> keySerializer;
    protected final int maxEntries;
    protected int entries;
    protected final ByteBuffer writeBuffer;
    protected final int keySize;
    protected IndexEntry<K> first;
    protected IndexEntry<K> last;
    private final AtomicBoolean readOnly = new AtomicBoolean();

    protected Index(File file, long maxSize, int keySize, Serializer<K> keySerializer) {
        this.keySize = keySize;
        this.keySerializer = keySerializer;
        try {
            boolean newFile = file.createNewFile();
            if (newFile) {
                long alignedSize = align(maxSize);
                this.maxEntries = (int) (alignedSize / entrySize());
                this.writeBuffer = Buffers.allocate(entrySize(), false);
                this.storage = Storage.create(file, StorageMode.MMAP, alignedSize);
            } else { //existing file
                //empty buffer, no writes wil be allowed anyways
                this.storage = Storage.open(file, StorageMode.MMAP);
                this.writeBuffer = Buffers.allocate(0, false);
                long length = storage.length();
                if (length % entrySize() != 0) {
                    throw new IllegalStateException("Invalid index file length: " + length);
                }
                this.maxEntries = (int) (length / entrySize());
                this.entries = (int) (length / entrySize());

                var readBuffer = Buffers.allocate(entrySize(), false);
                this.first = readEntry(0, readBuffer);
                this.last = readEntry(entries - 1, readBuffer);
                readOnly.set(true);
            }

        } catch (IOException ioex) {
            throw new RuntimeException("Failed to create index", ioex);
        }
    }

    public void write(K key, long position) {
        if (readOnly.get()) {
            throw new RuntimeException("Index is read only");
        }
        requireNonNull(key, "Key must not be null");
        int compare = last.key.compareTo(key);
        if (compare > 0) {
            throw new IllegalArgumentException("Index entries must be ordered. Entry " + key + " must be greater than previous entry " + last.key);
        }
        if (compare == 0) {
            throw new IllegalArgumentException("Duplicate index entry " + key);
        }
        IndexEntry<K> entry = new IndexEntry<>(key, position);
        if (first == null) {
            first = entry;
        }
        last = entry;
        write(entry);
    }

    private void write(IndexEntry<K> entry) {
        keySerializer.writeTo(entry.key, writeBuffer);
        writeBuffer.putLong(entry.logPosition);
        writeBuffer.flip();
        storage.write(writeBuffer);
        writeBuffer.clear();
        entries++;
    }

    /**
     * Complete this index and mark it as read only.
     */
    public void complete() {
        readOnly.set(true);
    }

    public void flush() {

    }

    public IndexEntry<K> find(K key) {
        throw new UnsupportedOperationException("TODO");
    }

    /**
     * Returns the start slot position that the key is contained, null if the key is less than the first item
     */
    public IndexEntry<K> floor(K key) {
        requireNonNull(key, "Key must be provided");
        if (entries == 0) {
            return null;
        }
        if (key.compareTo(last.key) > 0) {
            return null; //greater than last entry
        }
        var readBuffer = Buffers.allocate(entrySize(), false);

        int low = 0;
        int high = entries - 1;

        IndexEntry<K> ie;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            readBuffer.clear();
            ie = readEntry(mid, readBuffer);
            int cmp = ie.compareTo(key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return ie; // key found, exact match
        }
        int idx = -(low + 1);
        int lower = Math.abs(idx) - 2;
        readBuffer.clear();
        return readEntry(lower, readBuffer);  // key not found, return the lower value (if possible)
    }

    public boolean isFull() {
        return entries == maxEntries;
    }

    public long entries() {
        return entries;
    }

    private int entrySize() {
        return keySize + Long.BYTES;
    }

    private IndexEntry<K> readEntry(int idx, ByteBuffer readBuffer) {
        if (idx >= entries) {
            throw new IllegalStateException("Index cannot be greater than " + entries + ", got " + idx);
        }
        storage.read(idx * entrySize(), readBuffer);
        readBuffer.flip();
        K key = keySerializer.fromBytes(readBuffer);
        long pos = readBuffer.getLong();
        return new IndexEntry<>(key, pos);
    }

    private long align(long size) {
        int entrySize = entrySize();
        return entrySize * (size / entrySize);
    }

    public void delete() {
        storage.delete();
    }
}
