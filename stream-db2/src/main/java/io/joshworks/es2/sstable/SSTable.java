package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.SegmentFile;
import io.joshworks.es2.index.BIndex;
import io.joshworks.es2.index.IndexEntry;
import io.joshworks.es2.index.IndexFunction;
import io.joshworks.es2.sink.Sink;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;

import static io.joshworks.es2.Event.NO_VERSION;

public class SSTable implements SegmentFile {

    public static final int NO_DATA = -11;
    private static final String INDEX_EXT = "idx";
    final SegmentChannel channel;
    final BIndex index;

    private SSTable(SegmentChannel channel, BIndex index) {
        this.channel = channel;
        this.index = index;
    }

    static SSTable open(File dataFile) {
        var indexFile = indexFile(dataFile);
        var data = SegmentChannel.open(dataFile);
        var index = BIndex.open(indexFile);
        return new SSTable(data, index);
    }

    static SSTable create(File dataFile, Iterator<ByteBuffer> items, int expectedEntries, long size, CompactionProfile config) {
        var indexFile = indexFile(dataFile);
        try (var dataChunkWriter = new StreamBlockWriter(dataFile, indexFile, size, config.codec, config.blockSize, config.bloomFilterFalsePositive, expectedEntries)) {
            while (items.hasNext()) {
                ByteBuffer data = items.next();
                dataChunkWriter.add(data);
            }
        }
        return new SSTable(SegmentChannel.open(dataFile), BIndex.open(indexFile));
    }

    static void writeBlocks(File dataFile, Iterator<ByteBuffer> blocks, int expectedEntries, double fpPercentage) {

        try (var dataChannel = SegmentChannel.create(dataFile);
             var indexWriter = BIndex.writer(indexFile(dataFile), expectedEntries, fpPercentage)) {

            while (blocks.hasNext()) {
                var block = blocks.next();

                var stream = StreamBlock.stream(block);
                var startVersion = StreamBlock.startVersion(block);
                var blockSize = StreamBlock.sizeOf(block);
                var blockEntries = StreamBlock.entries(block);
                long logPos = dataChannel.append(block);

                indexWriter.add(stream, startVersion, blockSize, blockEntries, logPos);
            }

            dataChannel.truncate();
        }
    }

    static File indexFile(File dataFile) {
        Path parent = dataFile.toPath().getParent();
        String indexFileName = dataFile.getName().split("\\.")[0] + "." + INDEX_EXT;
        return parent.resolve(indexFileName).toFile();
    }

    public int version(long stream) {
        IndexEntry ie = index.find(stream, Integer.MAX_VALUE, IndexFunction.FLOOR);
        return ie == null ? NO_VERSION : ie.version() + ie.entries() - 1;
    }

    public int get(long stream, int version, Sink sink) {
        if (!index.contains(stream, version)) {
            return NO_DATA;
        }
        IndexEntry ie = index.find(stream, version, IndexFunction.FLOOR);
        if (ie == null) {
            return NO_DATA;
        }

        int recSize = ie.recordSize();
        long logAddress = ie.logAddress();
        int startVersion = ie.version();
        int entries = ie.entries();

        assert startVersion <= version;

        //end version is greater than latest version from index, sstable should not continue searching (considering sstable lookup is older to newer)
        if (version > startVersion + entries - 1) {
            return Event.VERSION_TOO_HIGH;
        }
        //cast is ok since the data transferred is never going to be greater than stream block
        return (int) channel.transferTo(logAddress, recSize, sink);
    }

    public IndexEntry get(long stream, int version) {
        if (!index.contains(stream, version)) {
            return null;
        }
        return index.find(stream, version, IndexFunction.FLOOR);
    }

    public long sparseEntries() {
        return index.sparseEntries();
    }

    public long denseEntries() {
        return index.denseEntries();
    }

    @Override
    public void close() {
        channel.close();
        index.close();
    }

    @Override
    public void delete() {
        channel.delete();
        index.delete();
    }

    @Override
    public String name() {
        return channel.name();
    }

    @Override
    public long size() {
        return channel.size();
    }

    @Override
    public String toString() {
        return channel.toString();
    }

}
