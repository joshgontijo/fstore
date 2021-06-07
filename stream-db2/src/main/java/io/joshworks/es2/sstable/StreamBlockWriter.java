package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.StreamBlock;
import io.joshworks.es2.index.BIndex;
import io.joshworks.es2.index.IndexWriter;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

public class StreamBlockWriter {

    private final BlockCodec codec;
    //use do append event data to be compressed
    private final ByteBuffer rawChunkData;
    //StreamChunk header + compressed
    private final ByteBuffer chunk;


    Long currentStream = null;
    int chunkEntries = 0;
    int chunkStartVersion = -1;

    public StreamBlockWriter(BlockCodec codec, int chunkSize) {
        this.codec = codec;
        this.rawChunkData = Buffers.allocate(chunkSize, false);
        this.chunk = Buffers.allocate(StreamBlock.HEADER_BYTES + chunkSize, false);
    }

    public void clear() {
        currentStream = null;
        chunkEntries = 0;
        chunkStartVersion = -1;

        rawChunkData.clear();
        chunk.clear();

    }

    public void add(ByteBuffer data, SegmentChannel dataChannel, BIndex.Writer indexWriter) {
        assert data.remaining() <= rawChunkData.capacity();

        long stream = Event.stream(data);
        int version = Event.version(data);
        if (currentStream == null) { //init version
            chunkStartVersion = version;
            currentStream = stream;
        }
        if (data.remaining() > rawChunkData.remaining() || stream != currentStream) { // full chunk or different stream
            flushChunk(dataChannel, indexWriter);
            chunkStartVersion = version;
            currentStream = stream;
        }

        assert chunkStartVersion >= 0;
        int expected = chunkStartVersion + chunkEntries;
        assert expected == version : "Event version must be contiguous (" + expected + " / " + version + ")";

        Buffers.copy(data, rawChunkData);
        chunkEntries++;
    }

    public void complete(SegmentChannel dataChannel, BIndex.Writer indexWriter) {
        if (rawChunkData.position() > 0) {
            flushChunk(dataChannel, indexWriter);
        }
    }

    private void flushChunk(SegmentChannel channel, BIndex.Writer indexWriter) {
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

}
