package io.joshworks.es2;

import io.joshworks.es2.index.BTreeIndexSegment;
import io.joshworks.es2.index.IndexEntry;
import io.joshworks.es2.index.IndexFunction;
import io.joshworks.es2.index.IndexWriter;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;

public class SSTables {

    private static final String DATA_EXT = "sst";
    private static final String INDEX_EXT = "idx";
    private final Path folder;
    private final CopyOnWriteArrayList<SSTable> entries = new CopyOnWriteArrayList<>();

    public SSTables(Path folder) {
        this.folder = folder;
    }

    private void openFiles() {
        for (File file : requireNonNull(folder.toFile().listFiles())) {
            if (!file.getName().endsWith(DATA_EXT)) {
                continue;
            }


            SSTable.open(file);
        }
    }

    private static void

    public IndexEntry get(long stream, int fromVersionInclusive) {
        for (SSTable sstable : entries) {
            var ie = sstable.get(stream, fromVersionInclusive);
            if (ie != null) {
                return ie;
            }
        }
        return null;
    }

    public int version(long stream) {
        for (SSTable sstable : entries) {
            var ie = sstable.get(stream, Integer.MAX_VALUE);
            if (ie != null) {
                return ie.version() + ie.entries() - 1;
            }
        }
        return -1;
    }


    public void flush(Iterator<ByteBuffer> iterator) {

        SegmentChannel dataChannel = null; //TODO
        SegmentChannel indexChannel = null; //TODO
        IndexWriter indexWriter = new IndexWriter(indexChannel);

        ByteBuffer streamChunk = ByteBuffer.allocate(Memory.PAGE_SIZE);
        ByteBuffer compressedChunk = ByteBuffer.allocate(Memory.PAGE_SIZE);
        Codec codec = new SnappyCodec();

        long lastStream = 0;
        int chunkEntries = 0;
        int chunkStartVersion = -1;

        while (iterator.hasNext()) {
            ByteBuffer data = iterator.next();

            long stream = Event.stream(data);

            if (data.remaining() > streamChunk.capacity()) { //bigger than block, flush data
                int version = Event.version(data);
                flushChunk(dataChannel, indexWriter, codec, stream, 1, version, data.flip(), compressedChunk);
                continue; //do not proceed as there's no need to copy or assign anything
            }
            if (data.remaining() > streamChunk.remaining() || stream != lastStream) { // full chunk or different stream
                flushChunk(dataChannel, indexWriter, codec, lastStream, chunkEntries, chunkStartVersion, streamChunk.flip(), compressedChunk);
            }

            if (data.position() == 0) { //init version
                chunkStartVersion = Event.version(data);
                lastStream = stream;
            }

            Buffers.copy(data, streamChunk);
            chunkEntries++;
        }
        if (streamChunk.position() > 0) {
            flushChunk(dataChannel, indexWriter, codec, lastStream, chunkEntries, chunkStartVersion, streamChunk.flip(), compressedChunk);
        }
    }

    private void flushChunk(SegmentChannel channel, IndexWriter indexWriter, Codec codec, long stream, int chunkEntries, int chunkStartVersion, ByteBuffer data, ByteBuffer compressedData) {
        assert chunkStartVersion >= 0;

        codec.compress(data, compressedData);
        ByteBuffer compressed = compressedData.flip();
        int recordSize = compressed.remaining();
        long logPos = channel.append(compressed);
        indexWriter.add(stream, chunkStartVersion, recordSize, chunkEntries, logPos);
        compressedData.clear();
        data.clear();
    }

    private static class SSTable {
        private final SegmentChannel channel;
        private final BTreeIndexSegment index;

        private SSTable(SegmentChannel channel, BTreeIndexSegment index) {
            this.channel = channel;
            this.index = index;
        }

        private static SSTable create(SegmentChannel dataChannel, File indexFile) {
            var channel = dataChannel;
            var index = BTreeIndexSegment.open(indexFile);
            return new SSTable(channel, index);
        }

        private static SSTable open(File dataFile, File indexFile) {
            var channel = SegmentChannel.open(dataFile);
            var index = new BTreeIndexSegment(indexFile, );
            return new SSTable(channel, index);
        }

        public IndexEntry get(long stream, int version) {
            return index.find(stream, version, IndexFunction.FLOOR);
        }
    }
}
