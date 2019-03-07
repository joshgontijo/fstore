package io.joshworks.fstore.lsmtree.sstable.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.extra.DataFile;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Index<K extends Comparable<K>> implements Closeable {

    private final List<IndexEntry<K>> entries;
    private final DataFile<IndexEntry<K>> dataFile;

    private boolean dirty;

    public Index(File indexDir, String segmentFileName, Serializer<K> keySerializer, String magic) {
        File file = getFile(indexDir, segmentFileName);
        this.dataFile = DataFile.of(new IndexEntrySerializer<>(keySerializer)).withMagic(magic).open(file);
        this.entries = load(file);
    }

    public void add(K key, long pos) {
        Objects.requireNonNull(key, "Index key cannot be null");
        this.entries.add(new IndexEntry<>(key, pos));
        dirty = true;
    }

    public void write() {
        if (!dirty) {
            return;
        }
        for (IndexEntry<K> indexEntry : entries) {
            dataFile.add(indexEntry);
        }
        dirty = false;
    }

    private List<IndexEntry<K>> load(File handler) {
        if (!handler.exists()) {
            return new ArrayList<>();
        }

        List<IndexEntry<K>> loaded = new ArrayList<>();
        try (LogIterator<IndexEntry<K>> iterator = dataFile.iterator(Direction.FORWARD)) {
            while (iterator.hasNext()) {
                loaded.add(iterator.next());
            }
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
        return loaded;
    }

    private static File getFile(File indexDir, String segmentName) {
        return new File(indexDir, segmentName.split("\\.")[0] + ".idx");
    }

    private int getMidpointIdx(K entry) {
        int idx = Collections.binarySearch(entries, entry);
        if (idx < 0) {
            idx = Math.abs(idx) - 2; // -1 for the actual position, -1 for the offset where to start scanning
            idx = idx < 0 ? 0 : idx;
        }
        if (idx >= entries.size()) {
            throw new IllegalStateException("Got index " + idx + " index position: " + entries.size());
        }
        return idx;
    }

    public IndexEntry get(K entry) {
        int midpointIdx = getMidpointIdx(entry);
        if (midpointIdx >= entries.size() || midpointIdx < 0) {
            return null;
        }
        return entries.get(midpointIdx);
    }

    public void roll() {
        dataFile.markAsReadOnly();
    }

    public void delete() {
        dataFile.delete();
    }

    public int size() {
        return entries.size();
    }

    public K first() {
        if (entries.isEmpty()) {
            return null;
        }
        return firstMidpoint().key;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public K last() {
        if (entries.isEmpty()) {
            return null;
        }
        return lastMidpoint().key;
    }

    private IndexEntry<K> firstMidpoint() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(0);
    }

    private IndexEntry<K> lastMidpoint() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(entries.size() - 1);
    }


    @Override
    public void close() {
        dataFile.close();
    }
}
