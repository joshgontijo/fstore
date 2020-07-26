package io.joshworks.es.reader;

import io.joshworks.es.Event;
import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryPlanner {

    //not static
    private final ThreadLocal<ByteBuffer> pageBufferCache;

    private final Index index;
    private final Log log;

    private List<PageEntry> entries = new ArrayList<>();
    private long stream;
    private int startVersion;
    private ByteBuffer pageBuffer;

    QueryPlanner(Index index, Log log, int pageBufferSize) {
        this.index = index;
        this.log = log;
        this.pageBufferCache = ThreadLocal.withInitial(() -> Buffers.allocate(pageBufferSize, false));
    }

    public boolean prepare(long stream, int version, int maxItems) {
        this.pageBuffer = pageBufferCache.get().clear();
        this.stream = stream;
        this.startVersion = version;

        this.entries = readKeys(index, stream, version, maxItems, pageBuffer.remaining());
        return !entries.isEmpty();
    }

    public int execute(ByteBuffer dst) {
        PageEntry first = entries.get(0);
        log.read(first.entry.logAddress(), pageBuffer);
        pageBuffer.flip();

        int idx = 0;
        int version = startVersion;
        int totalRead = 0;
        while (Event.isValid(pageBuffer) && idx < entries.size()) {
            int size = Event.sizeOf(pageBuffer);
            PageEntry entry = entries.get(idx);
            if (!expectedEvent(version) || !matchesOffset(entry.offsetInPage, pageBuffer.position())) {
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

    private boolean matchesOffset(long offsetInPage, int position) {
        return offsetInPage == position;
    }

    private boolean expectedEvent(int version) {
        return Event.stream(pageBuffer) == stream && Event.version(pageBuffer) == version;
    }

    private List<PageEntry> readKeys(Index index, long stream, int version, int maxItems, int readSize) {
        List<PageEntry> entries = new ArrayList<>();

        long startAddress = Long.MAX_VALUE;
        do {
            List<IndexEntry> keyBatch = index.get(new IndexKey(stream, version), maxItems - entries.size());
            if (keyBatch.isEmpty()) {
                return entries;
            }
            for (IndexEntry entry : keyBatch) {
                startAddress = Math.min(startAddress, entry.logAddress());
                long diff = posDiff(startAddress, entry.logAddress());
                if (diff == -1 || diff > readSize) {
                    return entries;
                }
                entries.add(new PageEntry(entry, diff));
                version++;
            }


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
