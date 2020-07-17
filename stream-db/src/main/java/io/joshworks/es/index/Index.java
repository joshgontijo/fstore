package io.joshworks.es.index;

import io.joshworks.es.index.tree.RedBlackBST;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Index {

    private final List<IndexSegment> indexes = new CopyOnWriteArrayList<>();

    //index has address like log
    //X bytes for index idx
    //64-X bytes for idx inside index

    //TODO add memtable
    private final RedBlackBST memTable;
    private final int maxEntries;

    public Index(int maxEntries) {
        this.maxEntries = maxEntries;
        memTable = new RedBlackBST(maxEntries);
    }

    public void append(long stream, int version, int size, long logPos) {
        if(memTable.isFull()) {
            roll();
        }
        memTable.put(stream, version, size, logPos);

    }

    private void roll() {
        IndexSegment head = new IndexSegment();
        head.append(stream, version, size, logPos);
    }

    public long find(long stream, int version, IndexFunction fn) {
        for (int i = indexes.size() - 1; i >= 0; i--) {
            IndexSegment index = indexes.get(i);
            int entryIdx = index.find(stream, version, fn);
            if (entryIdx != IndexSegment.NONE) {
                return toIndexAddress(i, entryIdx);
            }
        }
        return IndexSegment.NONE;
    }

    private long toIndexAddress(int segIdx, int entryIdx) {
        return 0; //todo
    }

    private int segmentIdx(long address) {
        return 0; //todo
    }

    private int entryIdx(long address) {
        return 0; //todo
    }

    public int version(long address) {
        int segIdx = segmentIdx(address);
        int entryIdx = entryIdx(address);
        return indexes.get(segIdx).version(entryIdx);
    }

    public long stream(long address) {
        int segIdx = segmentIdx(address);
        int entryIdx = entryIdx(address);
        return indexes.get(segIdx).stream(entryIdx);
    }

    public int size(long address) {
        int segIdx = segmentIdx(address);
        int entryIdx = entryIdx(address);
        return indexes.get(segIdx).size(entryIdx);
    }

    public long logAddress(long address) {
        int segIdx = segmentIdx(address);
        int entryIdx = entryIdx(address);
        return indexes.get(segIdx).logPos(entryIdx);
    }

}
