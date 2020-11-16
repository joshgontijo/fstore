package io.joshworks.es2;

import io.joshworks.es2.writer.BufferedWriter;
import io.joshworks.fstore.core.util.Memory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class SSTables {

    private final Path folder;
    private final CopyOnWriteArrayList<SSTable> entries = new CopyOnWriteArrayList<>();

    private final ByteBuffer writeDataBuffer = ByteBuffer.allocate(Memory.PAGE_SIZE);
    private final ByteBuffer writeIndexBuffer = ByteBuffer.allocate(Memory.PAGE_SIZE);

    public SSTables(Path folder) {
        this.folder = folder;
    }

    public int version(long stream) {
        for (SSTable log : entries) {

        }
    }

    public void flush(Iterator<ByteBuffer> iterator, int entries, long size) {

        SegmentChannel dataChannel = SegmentChannel.create();
        SegmentChannel indexChannel = SegmentChannel.create();

        List<ByteBuffer> indexBlocks = new ArrayList<>();

        while (iterator.hasNext()) {
            ByteBuffer data = iterator.next();
            BufferedWriter.write(dataChannel, data, writeDataBuffer);
        }
        BufferedWriter.flush(dataChannel, writeDataBuffer);

    }


    private static class SSTable {
        private final SegmentChannel channel;
        private final SegmentChannel.MappedReadRegion index;

        private SSTable(SegmentChannel channel) {
            this.channel = channel;
        }

        private int version(long stream) {

        }

    }
}
