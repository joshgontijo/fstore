package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

//File does not support reopening, must be created from scratch every time a segment is created
public class SparseIndex<K extends Comparable<K>> {

    private final Storage storage;
    private final int minSparseness;
    private final Serializer<K> keySerializer;
    private final int maxEntries;
    private int entries;
    private final ByteBuffer writeBuffer;
    private final int keySize;
    private IndexEntry<K> first;
    private IndexEntry<K> last;
    private long lastIndexWrite;

    private final List<IndexEntry<K>> _entries = new ArrayList<>();

    protected SparseIndex(File file, long maxSize, int keySize, int minSparseness, Serializer<K> keySerializer) {
        this.keySize = keySize;
        this.minSparseness = minSparseness;
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
            }

        } catch (IOException ioex) {
            throw new RuntimeException("Failed to create index", ioex);
        }
    }

    public void add(K key, long position) {
        IndexEntry<K> entry = new IndexEntry<>(key, position);
        if (first == null) {
            first = entry;
        }
        last = entry;
        if (position == 0 || (position - lastIndexWrite) >= minSparseness) {
            write(entry);
        }
    }

    private void write(IndexEntry<K> entry) {
        keySerializer.writeTo(entry.key, writeBuffer);
        writeBuffer.putLong(entry.logPosition);
        writeBuffer.flip();
        storage.write(writeBuffer);
        writeBuffer.clear();
        entries++;
        lastIndexWrite = entry.logPosition;
        _entries.add(entry);
    }

    public void complete() {
        //write only if it hasn't been written to disk
        if (last.logPosition > lastIndexWrite) {
            write(last);
        }
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
