package io.joshworks.es;

import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryPlanner {

    private List<ReadChunk> chunks;

    public boolean prepare(Index index, IndexKey key, int maxEntries, int maxBytes) {
        List<IndexEntry> entries = readKeys(index, key, maxEntries);

        if (entries.isEmpty()) {
            return false;
        }

        if (entries.get(0).size() > maxBytes) {
            throw new IllegalStateException("Not enough destination buffer space");
        }

        this.chunks = computeReadChunks(entries, maxBytes);
        assert chunks.size() > 0 : "no data chunk to be read";
        return true;
    }

    public int execute(Log log, ByteBuffer dst) {
        int plim = dst.limit();
        int ppos = dst.position();

        for (ReadChunk chunk : chunks) {
            int bufferChunkStart = dst.position();
            Buffers.offsetLimit(dst, chunk.size);
            int read = log.read(chunk.startPos, dst);
            assert chunk.size == read;
            rewriteEntries(dst, bufferChunkStart, chunk);
        }

        dst.limit(plim);

        int totalRead = dst.position() - ppos;
        assert totalRead == chunks.stream().mapToInt(c -> c.size).sum();

        return totalRead;
    }

    private List<IndexEntry> readKeys(Index index, IndexKey key, int count) {
        List<IndexEntry> entries = new ArrayList<>();
        int version = key.version();
        IndexEntry ie;
        do {
            ie = index.find(new IndexKey(key.stream(), version), IndexFunction.EQUALS);
            if (ie != null) {
                entries.add(ie);
                version++;
            }
        } while (ie != null && entries.size() < count);

        return entries;
    }

    private List<ReadChunk> computeReadChunks(List<IndexEntry> entries, int dstAvailable) {
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }
        int readTotal = 0;
        List<ReadChunk> chunks = new ArrayList<>();
        ReadChunk chunk = null;
        for (IndexEntry entry : entries) {
            if (readTotal + entry.size() > dstAvailable) {
                break;
            }
            readTotal += entry.size();
            if (chunk == null) {
                chunk = new ReadChunk(entry);
                continue;
            }
            if (!chunk.add(entry)) {
                chunks.add(chunk);
                chunk = null;
            }
        }
        assert chunk != null;
        chunks.add(chunk);

        return chunks;
    }

    private void rewriteEntries(ByteBuffer dst, int startPos, ReadChunk chunk) {
        int evOffset = startPos;
        int version = chunk.startVersion;
        for (int i = 0; i < chunk.entries; i++) {
            Event.rewrite(dst, evOffset, chunk.stream, version);
            evOffset += Event.sizeOf(dst, evOffset);
            version++;
        }
    }

    private static class ReadChunk {
        private final long stream;
        private final long startPos;
        private final int startVersion;
        private int size;
        private int entries = 1;

        private ReadChunk(IndexEntry entry) {
            this.stream = entry.stream();
            this.startPos = entry.logAddress();
            this.startVersion = entry.version();
            this.size = entry.size();
        }

        public boolean add(IndexEntry entry) {
            if (startPos + size != entry.logAddress()) {
                return false;
            }
            this.size += entry.size();
            entries++;
            return true;
        }
    }


}
