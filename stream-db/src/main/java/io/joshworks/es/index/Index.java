package io.joshworks.es.index;

import io.joshworks.es.SegmentDirectory;
import io.joshworks.es.index.btree.BTreeIndexSegment;
import io.joshworks.es.index.tree.Node;
import io.joshworks.es.index.tree.RedBlackBST;

import java.io.File;

public class Index extends SegmentDirectory<BTreeIndexSegment> {

    public static final int BLOCK_SIZE = 4096;
    private final RedBlackBST memTable;
    private final int maxEntries;
    private final int midPointFactor;

    static final String EXT = "idx";

    public static final int NONE = -1;

    public Index(File root, int maxEntries, int midPointFactor) {
        super(root, EXT);
        this.memTable = new RedBlackBST(maxEntries);
        this.maxEntries = maxEntries;
        this.midPointFactor = midPointFactor;
//        super.loadSegments(f -> new IndexSegment(f, -1, midPointFactor));
        super.loadSegments(f -> new BTreeIndexSegment(f, maxEntries, BLOCK_SIZE));
    }

    public void append(long stream, int version, int size, long logPos) {
        if (memTable.isFull()) {
            flush();
        }
        memTable.put(new IndexEntry(stream, version, size, logPos));
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
        BTreeIndexSegment index = new BTreeIndexSegment(newSegmentFile(0), maxEntries, BLOCK_SIZE);
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
