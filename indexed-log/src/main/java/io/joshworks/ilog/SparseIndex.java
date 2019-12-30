package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

//File does not support reopening, must be created from scratch every time a segment is created
public class SparseIndex<K extends Comparable<K>> {

    private final Storage storage;
    private final Serializer<K> keySerializer;
    private final int maxEntries;
    private int entries;
    private final ByteBuffer writeBuffer;
    private final int keySize;
    private IndexEntry<K> first;

    protected SparseIndex(File file, long maxSize, int keySize, Serializer<K> keySerializer) {
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
            }

        } catch (IOException ioex) {
            throw new RuntimeException("Failed to create index", ioex);
        }
    }

    public void write(K key, long position) {
        keySerializer.writeTo(key, writeBuffer);
        writeBuffer.putLong(position);
        writeBuffer.flip();
        storage.write(writeBuffer);
        writeBuffer.clear();
        entries++;

        IndexEntry<K> ie = new IndexEntry<>(key, position);
        if (first == null) {
            first = ie;
        }
    }

    /**
     * Returns the start slot position that the key is contained, null if the key is less than the first item
     */
    public IndexEntry<K> floor(K key) {

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

    public IndexEntry<K> first() {
        return first;
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
