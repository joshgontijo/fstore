package io.joshworks.es.index;

import io.joshworks.es.SegmentDirectory;
import io.joshworks.es.index.btree.BTreeIndexSegment;
import io.joshworks.es.index.tree.Node;
import io.joshworks.es.index.tree.RedBlackBST;
import io.joshworks.fstore.core.cache.Cache;

import java.io.File;

public class Index extends SegmentDirectory<BTreeIndexSegment> {

    private final RedBlackBST memTable;
    private final int maxEntries;
    private final int blockSize;

    static final String EXT = "idx";

    public static final int NONE = -1;

    private final Cache<Long, Integer> versionCache;

    public Index(File root, int maxEntries, int blockSize, int versionCacheSize) {
        super(root, EXT);
        if (blockSize % 2 != 0) {
            throw new IllegalArgumentException("Block size must be power of two, got " + blockSize);
        }
        if (blockSize > Short.MAX_VALUE) { //block uses short for BLOCK_SIZE
            throw new IllegalArgumentException("Block size must cannot be greater than " + Short.MAX_VALUE);
        }
        this.memTable = new RedBlackBST(maxEntries);
        this.maxEntries = maxEntries;
        this.blockSize = blockSize;
        this.versionCache = Cache.lruCache(versionCacheSize, -1);
        super.loadSegments(f -> new BTreeIndexSegment(f, maxEntries, blockSize));
    }

    public void append(IndexEntry entry) {
        if (memTable.isFull()) {
            flush();
        }
        memTable.put(entry);
    }

    public int version(long stream) {
        Integer cached = versionCache.get(stream);
        if (cached != null) {
            return cached;
        }
        IndexEntry ie = find(IndexKey.maxOf(stream), IndexFunction.FLOOR);
        if (ie == null || ie.stream() != stream) {
            return -1;
        }
        int version = ie.version();
        versionCache.add(stream, version);
        return version;
    }

    public IndexEntry find(IndexKey key, IndexFunction fn) {
        Node found = memTable.apply(key, fn);
        if (found != null) {
            return new IndexEntry(found.stream, found.version, found.recordSize, found.logAddress);
        }

        //TODO support sparse segmentIdx
        for (int i = segments.size() - 1; i >= 0; i--) {
            BTreeIndexSegment index = segments.get(i);
            IndexEntry ie = index.find(key, fn);
            if (ie != null) {
                return ie;
            }
        }
        return null;
    }

    public long entries() {
        return segments.stream().mapToLong(BTreeIndexSegment::entries).sum() + memTable.entries();
    }

    public void flush() {
        System.out.println("Flushing memtable: " + memTable.entries());
        long start = System.currentTimeMillis();
        if (memTable.isEmpty()) {
            return;
        }
        BTreeIndexSegment index = new BTreeIndexSegment(newSegmentFile(0), maxEntries, blockSize);
        for (Node node : memTable) {
            index.append(node.stream, node.version, node.recordSize, node.logAddress);
        }

        index.complete();

        segments.add(index);
        makeHead(index);
        memTable.clear();

        System.out.println("Memtable flush complete in " + (System.currentTimeMillis() - start) + "ms");
    }

}
