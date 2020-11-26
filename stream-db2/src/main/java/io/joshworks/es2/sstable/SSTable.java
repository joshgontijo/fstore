package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;
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
import java.util.Iterator;

import static io.joshworks.es2.Event.NO_VERSION;

class SSTable {

    private final SegmentChannel data;
    private final BTreeIndexSegment index;

    private SSTable(SegmentChannel data, BTreeIndexSegment index) {
        this.data = data;
        this.index = index;
    }

    static SSTable open(File dataFile, File indexFile) {
        var data = SegmentChannel.open(dataFile);
        var index = BTreeIndexSegment.open(indexFile);
        return new SSTable(data, index);
    }

    public int version(long stream) {
        IndexEntry ie = index.find(stream, Integer.MAX_VALUE, IndexFunction.FLOOR);
        return ie == null ? NO_VERSION : ie.version();
    }

    public IndexEntry get(long stream, int version) {
        return index.find(stream, version, IndexFunction.FLOOR);
    }

    static SSTable create(File dataFile, File indexFile, Iterator<ByteBuffer> items) {
        SegmentChannel dataChannel = SegmentChannel.create(dataFile);
        SegmentChannel indexChannel = SegmentChannel.create(indexFile);
        IndexWriter indexWriter = new IndexWriter(indexChannel);

        ByteBuffer streamChunk = ByteBuffer.allocate(Memory.PAGE_SIZE);
        ByteBuffer compressedChunk = ByteBuffer.allocate(Memory.PAGE_SIZE);
        Codec codec = new SnappyCodec();

        long lastStream = 0;
        int chunkEntries = 0;
        int chunkStartVersion = -1;

        while (items.hasNext()) {
            ByteBuffer data = items.next();

            long stream = Event.stream(data);

            if (data.remaining() > streamChunk.capacity()) { //bigger than block, flush data
                int version = Event.version(data);
                flushChunk(dataChannel, indexWriter, codec, stream, 1, version, data.flip(), compressedChunk);
                continue; //do not proceed as there's no need to copy or assign anything
            }

            if (data.position() == 0) { //init version
                chunkStartVersion = Event.version(data);
                lastStream = stream;
            }

            if (data.remaining() > streamChunk.remaining() || stream != lastStream) { // full chunk or different stream
                flushChunk(dataChannel, indexWriter, codec, lastStream, chunkEntries, chunkStartVersion, streamChunk.flip(), compressedChunk);
            }

            Buffers.copy(data, streamChunk);
            chunkEntries++;
        }
        if (streamChunk.position() > 0) {
            flushChunk(dataChannel, indexWriter, codec, lastStream, chunkEntries, chunkStartVersion, streamChunk.flip(), compressedChunk);
        }

        dataChannel.truncate();
        indexChannel.truncate();
        return new SSTable(dataChannel, BTreeIndexSegment.open(indexFile));
    }

    private static void flushChunk(SegmentChannel channel, IndexWriter indexWriter, Codec codec, long stream, int chunkEntries, int chunkStartVersion, ByteBuffer data, ByteBuffer compressedData) {
        assert chunkStartVersion >= 0;

        codec.compress(data, compressedData);
        ByteBuffer compressed = compressedData.flip();
        int chunkSize = compressed.remaining();
        long logPos = channel.append(compressed);
        indexWriter.add(stream, chunkStartVersion, chunkSize, chunkEntries, logPos);
        compressedData.clear();
        data.clear();
    }

}
