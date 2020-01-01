package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;

import java.io.File;

//File does not support reopening, must be created from scratch every time a segment is created
public class SparseIndex<K extends Comparable<K>> extends Index<K> {

    private final int minSparseness;
    private long lastIndexWrite;

    protected SparseIndex(File file, long maxSize, int keySize, Serializer<K> keySerializer, int minSparseness) {
        super(file, maxSize, keySize, keySerializer);
        this.minSparseness = minSparseness;
    }

    public void write(K key, long position) {
        last = new IndexEntry<>(key, position);
        if (position == 0 || (position - lastIndexWrite) >= minSparseness) {
            super.write(key, position);
            lastIndexWrite = position;
        }
    }

    public void complete() {
        //write only if it hasn't been written to disk
        if (last.logPosition > lastIndexWrite) {
            super.write(last.key, last.logPosition);
        }
    }
}
