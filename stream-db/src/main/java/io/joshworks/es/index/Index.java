package io.joshworks.es.index;

import io.joshworks.es.SegmentDirectory;
import io.joshworks.es.index.btree.BTreeIndexSegment;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Index extends SegmentDirectory<BTreeIndexSegment> {

    private final Map<Long, List<IndexEntry>> table = new ConcurrentHashMap<>();
    private final AtomicInteger entries = new AtomicInteger();
    private final int maxEntries;
    private final int blockSize;

    static final String EXT = "idx";

    public static final int NONE = -1;

    public Index(File root, int maxEntries, int blockSize) {
        super(root, EXT, Long.MAX_VALUE);
        if (blockSize % 2 != 0) {
            throw new IllegalArgumentException("Block size must be power of two, got " + blockSize);
        }
        if (blockSize > Short.MAX_VALUE) { //block uses short for BLOCK_SIZE
            throw new IllegalArgumentException("Block size must cannot be greater than " + Short.MAX_VALUE);
        }
        this.maxEntries = maxEntries;
        this.blockSize = blockSize;
        super.loadSegments(f -> new BTreeIndexSegment(f, maxEntries, blockSize));
    }

    public void append(IndexEntry entry) {
        table.compute(entry.stream(), (k, v) -> {
            if (v == null) v = new ArrayList<>();
            v.add(entry);
            return v;
        });
        entries.incrementAndGet();
    }

    public int version(long stream) {
        List<IndexEntry> entries = table.get(stream);
        if (entries != null) {
            return entries.get(entries.size() - 1).version();
        }
        IndexEntry ie = readFromDisk(IndexKey.maxOf(stream), IndexFunction.FLOOR);
        if (ie == null || ie.stream() != stream) {
            return -1;
        }
        return ie.version();
    }

    public List<IndexEntry> get(IndexKey start, int count) {
        int version = start.version();

        List<IndexEntry> fromMemTable = new ArrayList<>();
        IndexEntry found;
        while ((found = getFromMemTable(start.stream(), version)) != null && fromMemTable.size() < count) {
            fromMemTable.add(found);
            version++;
        }
        if (!fromMemTable.isEmpty()) {
            return fromMemTable; //startVersion is in memtable, definitely nothing in disk
        }
        return readBatchFromDisk(start.stream(), version, count);
    }

    private List<IndexEntry> readBatchFromDisk(long stream, int version, int count) {
        if (count <= 0) {
            return Collections.emptyList();
        }

        for (long segIdx = headIdx(); segIdx >= 0; segIdx--) {
            BTreeIndexSegment index = super.tryGet(segIdx);
            if (index == null) { //gap, skip
                continue;
            }
            List<IndexEntry> entries = index.findBatch(stream, version, count);
            if (!entries.isEmpty()) {
                return entries;
            }
        }
        return Collections.emptyList();
    }

    public IndexEntry get(IndexKey key) {
        IndexEntry fromMemTable = getFromMemTable(key.stream(), key.version());
        if (fromMemTable != null) {
            return fromMemTable;
        }

        return readFromDisk(key, IndexFunction.EQUALS);
    }

    private IndexEntry readFromDisk(IndexKey key, IndexFunction fn) {
        for (long segIdx = headIdx(); segIdx >= 0; segIdx--) {
            BTreeIndexSegment index = super.tryGet(segIdx);
            if (index == null) {
                continue;
            }
            IndexEntry ie = index.find(key.stream(), key.version(), fn);
            if (ie != null) {
                return ie;
            }
        }
        return null;
    }

    private IndexEntry getFromMemTable(long stream, int version) {
        List<IndexEntry> entries = table.get(stream);
        if (entries == null) {
            return null;
        }
        int firstVersion = entries.get(0).version();
        int lastVersion = entries.get(entries.size() - 1).version();
        if (version < firstVersion || version > lastVersion) {
            return null;
        }
        int pos = version - firstVersion;
        return entries.get(pos);
    }

    public long entries() {
        long diskEntries = 0;
        for (int idx = 0; idx < headIdx(); idx++) {
            BTreeIndexSegment index = super.tryGet(idx);
            if (index == null) {
                continue;
            }
            diskEntries += index.entries();
        }
        return diskEntries + this.entries.get();
    }

    public boolean isFull() {
        return entries.get() >= maxEntries;
    }

    public void flush() {
        int entries = this.entries.get();
        System.out.println("Flushing memtable: " + entries);
        long start = System.currentTimeMillis();
        if (entries == 0) {
            return;
        }
        File headFile = newHeadFile();
        BTreeIndexSegment index = new BTreeIndexSegment(headFile, entries, blockSize);
        table.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry::getKey))
                .forEach(entry -> {
                    for (IndexEntry ie : entry.getValue()) {
                        index.append(ie.stream(), ie.version(), ie.logAddress());
                    }
                });

        index.complete();

        super.add(index);
        this.entries.set(0);
        table.clear();

        System.out.println("Memtable flush complete in " + (System.currentTimeMillis() - start) + "ms");
    }

}
