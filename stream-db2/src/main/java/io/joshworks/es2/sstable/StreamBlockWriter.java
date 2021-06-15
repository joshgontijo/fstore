package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.index.BIndex;
import io.joshworks.es2.index.BPTreeIndex;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;

public class StreamBlockWriter implements Closeable {

    private final SegmentChannel channel;
    private final BPTreeIndex.Writer indexWriter;
    private final BlockCodec codec;
    //use do append event data to be compressed
    private final ByteBuffer rawChunkData;
    //StreamChunk header + compressed
    private final ByteBuffer chunk;

    Long currentStream = null;
    int chunkEntries = 0;
    int chunkStartVersion = -1;

    public StreamBlockWriter(File dataFile, File indexFile, BlockCodec codec, int blockSize, double fpPercentage, int expectedEntries) {
        this.channel = SegmentChannel.create(dataFile);
        this.indexWriter = BPTreeIndex.writer(indexFile, expectedEntries, fpPercentage);
        this.codec = codec;
        this.rawChunkData = Buffers.allocate(blockSize, false);
        this.chunk = Buffers.allocate(StreamBlock.HEADER_BYTES + blockSize, false);
    }

    public void add(ByteBuffer data) {
        assert data.remaining() <= rawChunkData.capacity(); //TODO data cannot be greater than block size ?

        long stream = Event.stream(data);
        int version = Event.version(data);
        if (currentStream == null) { //init version
            chunkStartVersion = version;
            currentStream = stream;
        }

        indexWriter.stampEntry(stream, version);

        if (data.remaining() > rawChunkData.remaining() || stream != currentStream) { // full chunk or different stream
            flushChunk();
            chunkStartVersion = version;
            currentStream = stream;
        }

        assert chunkStartVersion >= 0;
        int expected = chunkStartVersion + chunkEntries;
        assert expected == version : "Event version must be contiguous (" + expected + " / " + version + ")";

        Buffers.copy(data, rawChunkData);
        chunkEntries++;
    }

    private void clear() {
        currentStream = null;
        chunkEntries = 0;
        chunkStartVersion = -1;

        rawChunkData.clear();
        chunk.clear();

    }

    private void flushChunk() {
        assert chunkStartVersion >= 0;
        assert rawChunkData.position() > 0;

        rawChunkData.flip();
        int uncompressedSize = rawChunkData.remaining();
        chunk.position(StreamBlock.HEADER_BYTES);

        codec.codec.compress(rawChunkData, chunk);
        chunk.flip();

        //STREAM BLOCK WRITE
        StreamBlock.writeHeader(chunk, currentStream, chunkStartVersion, chunkEntries, uncompressedSize, codec);

        int chunkSize = chunk.remaining();
        long logPos = channel.append(chunk);
        indexWriter.add(currentStream, chunkStartVersion, chunkSize, chunkEntries, logPos);
        clear();
    }

    @Override
    public void close() {
        if (rawChunkData.position() > 0) {
            flushChunk();
        }
        indexWriter.close();
        channel.truncate();
        channel.close();
    }
}
