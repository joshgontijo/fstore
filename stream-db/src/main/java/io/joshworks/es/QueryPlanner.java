package io.joshworks.es;

import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryPlanner {

    private List<PageEntry> entries;
    private long stream;
    private int startVersion;
    private ByteBuffer pageBuffer;

    public boolean prepare(Index index, IndexKey key, int maxItems, ByteBuffer pageBuffer) {
        this.pageBuffer = pageBuffer.clear();
        this.stream = key.stream();
        this.startVersion = key.version();

        this.entries = readKeys(index, key, maxItems, pageBuffer.remaining());
        return !entries.isEmpty();
    }

    public int execute(Log log, ByteBuffer dst) {

        PageEntry first = entries.get(0);
        int read = log.read(first.entry.logAddress(), pageBuffer);
        pageBuffer.flip();

        int idx = 0;
        int version = startVersion;
        int totalRead = 0;
        while (Event.isValid(pageBuffer)) {
            int size = Event.sizeOf(pageBuffer);
            PageEntry entry = entries.get(idx);
            if (entry.offsetInPage != pageBuffer.position()) {
                Buffers.offsetPosition(pageBuffer, size);
                continue;
            }
            if (dst.remaining() < size) {
                return totalRead;
            }
            Event.rewrite(pageBuffer, pageBuffer.position(), stream, version);
            totalRead += Buffers.copy(pageBuffer, pageBuffer.position(), size, dst);
            Buffers.offsetPosition(pageBuffer, size);
            version++;
            idx++;
        }

        return totalRead;
    }

    private List<PageEntry> readKeys(Index index, IndexKey key, int maxItems, int readSize) {
        List<PageEntry> entries = new ArrayList<>();

        IndexEntry ie = index.get(key);
        if (ie == null) {
            return entries;
        }
        entries.add(new PageEntry(ie, 0));

        long startAddress = ie.logAddress();

        int version = key.version();
        do {
            version++;
            IndexEntry next = index.get(new IndexKey(key.stream(), version));
            if (next == null) {
                return entries;
            }
            long diff = posDiff(startAddress, next.logAddress());
            if (diff == -1 || diff > readSize) {
                return entries;
            }
            entries.add(new PageEntry(next, diff));
        } while (entries.size() < maxItems);

        return entries;
    }

    private static long posDiff(long startAddress, long nextAddress) {
        int idx1 = Log.segmentIdx(startAddress);
        int idx2 = Log.segmentIdx(nextAddress);
        if (idx1 != idx2) { //different segments
            return -1;
        }

        long segPos1 = Log.positionOnSegment(startAddress);
        long segPos2 = Log.positionOnSegment(nextAddress);

        return segPos2 - segPos1;
    }

    private static class PageEntry {
        private final IndexEntry entry;
        private final long offsetInPage;

        private PageEntry(IndexEntry entry, long offsetInPage) {
            this.entry = entry;
            this.offsetInPage = offsetInPage;
        }
    }

}
