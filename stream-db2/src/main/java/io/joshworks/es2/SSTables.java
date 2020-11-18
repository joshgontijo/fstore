package io.joshworks.es2;

import io.joshworks.es2.index.BTreeIndexSegment;
import io.joshworks.es2.index.IndexEntry;
import io.joshworks.es2.index.IndexFunction;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

public class SSTables {

    private final Path folder;
    private final CopyOnWriteArrayList<SSTable> entries = new CopyOnWriteArrayList<>();

    public SSTables(Path folder) {
        this.folder = folder;
    }

    private void openFiles() {
        folder.
    }

    public IndexEntry get(long stream, int fromVersionInclusive) {
        for (SSTable sstable : entries) {
            var ie = sstable.get(stream, fromVersionInclusive);
            if (ie != null) {
                return ie;
            }
        }
        return null;
    }

    public void flush(Iterator<ByteBuffer> iterator, int entries, long size) {

        SSTable sstable = SSTable.create(entries, size);

        ByteBuffer streamChunk = ByteBuffer.allocate(Memory.PAGE_SIZE);
        ByteBuffer compressedChunk = ByteBuffer.allocate(Memory.PAGE_SIZE);
        long lastStream = 0;
        Codec codec = new SnappyCodec();

        while (iterator.hasNext()) {
            ByteBuffer data = iterator.next();

            long stream = Event.stream(data);
            if (data.remaining() > streamChunk.capacity()) {
                flushChunk(sstable, codec, data.flip(), compressedChunk);
                lastStream = stream;
            }
            if (data.remaining() > streamChunk.remaining() || stream != lastStream) {
                flushChunk(sstable, codec, streamChunk.flip(), compressedChunk);
                lastStream = stream;
            }

            Buffers.copy(data, streamChunk);
        }
        if (streamChunk.position() > 0) {
            flushChunk(sstable, codec, streamChunk.flip(), compressedChunk);
        }
    }

    private void flushChunk(SSTable sstable, Codec codec, ByteBuffer data, ByteBuffer compressedData) {
        codec.compress(data, compressedData);
        sstable.channel.append(compressedData.flip());
        compressedData.clear();
        data.clear();
    }


    private static class SSTable {
        private final SegmentChannel channel;
        private final BTreeIndexSegment index;

        public SSTable(SegmentChannel channel, BTreeIndexSegment index) {
            this.channel = channel;
            this.index = index;
        }

        private static SSTable create(int entries, long size) {
            var channel = SegmentChannel.create();
            var index = new BTreeIndexSegment(entries);
            return new SSTable(channel, index);
        }

        private static SSTable open(File dataFile) {
            var channel = SegmentChannel.create();
            var index = new BTreeIndexSegment(entries);
            return new SSTable(channel, index);
        }

        public IndexEntry get(long stream, int fromVersionInclusive) {
            return index.find(stream, fromVersionInclusive, IndexFunction.FLOOR);
        }
    }
}
