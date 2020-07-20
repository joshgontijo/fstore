package io.joshworks.es.index;

import io.joshworks.es.SegmentDirectory;
import io.joshworks.es.index.tree.Node;
import io.joshworks.es.index.tree.RedBlackBST;

import java.io.File;

public class Index extends SegmentDirectory<IndexSegment> {

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
        super.loadSegments(f -> new IndexSegment(f, -1, midPointFactor));
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
            IndexSegment index = segments.get(i);
            IndexEntry ie = index.find(key, fn);
            if (ie != null) {
                return ie;
            }
        }
        return null;
    }

    public long entries() {
        return segments.stream().mapToLong(IndexSegment::entries).sum() + memTable.entries();
    }

    public void flush() {
        System.out.println("Flushing memtable: " + memTable.entries());
        long start = System.currentTimeMillis();
        if (memTable.isEmpty()) {
            return;
        }
        IndexSegment index = new IndexSegment(newSegmentFile(0), maxEntries, midPointFactor);
        for (Node node : memTable) {
            index.append(node.stream, node.version, node.recordSize, node.logAddress);
        }

        index.flush();
        index.complete();

        segments.add(index);
        makeHead(index);
        memTable.clear();

        System.out.println("Memtable flush complete in " + (System.currentTimeMillis() - start) + "ms");
    }

}
