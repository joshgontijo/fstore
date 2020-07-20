package io.joshworks.es.index;

import io.joshworks.es.SegmentDirectory;
import io.joshworks.es.index.tree.Node;
import io.joshworks.es.index.tree.RedBlackBST;

import java.io.File;

public class Index extends SegmentDirectory<IndexSegment> {

    private final RedBlackBST memTable;
    private final int maxEntries;

    static final String EXT = "idx";

    public static final int NONE = -1;

    public Index(File root, int maxEntries) {
        super(root, EXT);
        memTable = new RedBlackBST(maxEntries);
        this.maxEntries = maxEntries;
        super.loadSegments(f -> new IndexSegment(f, -1));
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

    public void flush() {
        if (memTable.isEmpty()) {
            return;
        }
        IndexSegment index = new IndexSegment(newSegmentFile(0), maxEntries);
        for (Node node : memTable) {
            index.append(node.stream, node.version, node.recordSize, node.logAddress);
        }

        index.flush();
        index.complete();

        segments.add(index);
        makeHead(index);
        memTable.clear();
    }

}
