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
    private long entries;
    private final ByteBuffer writeBuffer;
    private final int keySize;
    private IndexEntry<K> first;
    private IndexEntry<K> last;

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
                this.first = readEntry(0);
                this.last = readEntry(maxEntries - 1);
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
        last = ie;
    }

    public IndexEntry<K> get(K key) {
        return binarySearch(key);
    }

    public IndexEntry<K> last() {
        return last;
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

    //returns the slot in which the key can be found, -1 if the key is out of range
    private IndexEntry<K> binarySearch(K key) {
        int low = 0;
        int high = maxEntries;

        IndexEntry<K> ie = null;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            ie = readEntry(mid);
            int cmp = ie.compareTo(key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return ie; // key found, exact match
        }
        //key not found, return the lower value (if possible)
        return ie;
//        if(ie == null) {
//            return null;
//        }
//        return -(low + 1);  // key not found, return the lower value (if possible)
    }

    private int entrySize() {
        return keySize + Long.BYTES;
    }

    private IndexEntry<K> readEntry(int idx) {
        var readBuffer = Buffers.allocate(entrySize(), false);
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
