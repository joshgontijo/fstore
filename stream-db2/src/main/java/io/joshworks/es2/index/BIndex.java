package io.joshworks.es2.index;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.SegmentFile;
import io.joshworks.es2.index.filter.BloomFilter;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Memory;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;

public class BIndex implements SegmentFile {

    private static final int FOOTER_SIZE = Long.BYTES * 2;

    //TODO add midpoints
    private final SegmentChannel.MappedReadRegion mf;
    private final SegmentChannel channel;
    private final BloomFilter filter;
    private final ByteBuffer keyBuffer = Buffers.allocate(IndexKey.BYTES, false); //Bloom filter only
    private final long denseEntries;

    private BIndex(SegmentChannel.MappedReadRegion mf, SegmentChannel channel, BloomFilter filter, long denseEntries) {
        this.mf = mf;
        this.channel = channel;
        this.filter = filter;
        this.denseEntries = denseEntries;
    }

    public static BIndex.Writer writer(File file, int expectedEntries, double fpPercentage) {
        return new Writer(file, expectedEntries, fpPercentage);
    }

    public static BIndex open(File file) {
        try {
            if (!file.exists()) {
                throw new RuntimeIOException("File does not exist");
            }

            var channel = SegmentChannel.open(file);

            //read footer
            ByteBuffer footer = Buffers.allocate(FOOTER_SIZE, false);
            int read = channel.read(footer, channel.position() - FOOTER_SIZE);
            assert read == FOOTER_SIZE;
            footer.flip();
            var indexSize = footer.getLong();
            var denseEntries = footer.getLong();

            //filter
            int filterSize = (int) (channel.size() - indexSize - FOOTER_SIZE);
            ByteBuffer filterBuf = Buffers.allocate(filterSize, false);
            read = channel.read(filterBuf, indexSize);
            assert read == filterSize;
            var filter = BloomFilter.load(filterBuf.flip());

            checkChannelSize(channel, indexSize);
            var mappedRegion = channel.map(0, (int) indexSize);

            return new BIndex(mappedRegion, channel, filter, denseEntries);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize index", e);
        }
    }

    private static void checkChannelSize(SegmentChannel channel, long indexSize) {
        if (indexSize > Buffers.MAX_CAPACITY) {
            channel.close();
            throw new RuntimeIOException("Index too big");
        }
        if (indexSize % IndexEntry.BYTES != 0) {
            channel.close();
            throw new IllegalStateException("Invalid index file length: " + indexSize);
        }
    }

    public IndexEntry find(long stream, int version, IndexFunction fn) {
        int idx = binarySearch(stream, version);
        idx = fn.apply(idx);
        return read(idx);
    }

    public boolean contains(long stream, int version) {
        return filter.contains(keyBuffer.clear().putLong(stream).putInt(version).flip());
    }

    private int binarySearch(long stream, int version) {
        int entries = sparseEntries();

        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int readPos = mid * IndexEntry.BYTES;
            int cmp = compare(readPos, stream, version);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    private IndexEntry read(int idx) {
        if (idx == -1) {
            return null;
        }
        var offset = idx * IndexEntry.BYTES;
        var stream = mf.getLong(offset);
        var version = mf.getInt(offset + Long.BYTES);
        var recSize = mf.getInt(offset + Long.BYTES + Integer.BYTES);
        var entries = mf.getInt(offset + Long.BYTES + Integer.BYTES + Integer.BYTES);
        var logPos = mf.getLong(offset + Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES);
        return new IndexEntry(stream, version, recSize, entries, logPos);
    }

    private int compare(int bufferPos, long keyStream, int keyVersion) {
        var stream = mf.getLong(bufferPos);
        var version = mf.getInt(bufferPos + Long.BYTES);
        return IndexKey.compare(stream, version, keyStream, keyVersion);
    }

    //actual index entries
    public int sparseEntries() {
        return mf.capacity() / IndexEntry.BYTES;
    }

    //dense entry count, used for filter or total count
    public long denseEntries() {
        return denseEntries;
    }

    @Override
    public void close() {
        mf.close();
        channel.close();
    }

    @Override
    public void delete() {
        mf.close();
        channel.delete();
    }

    @Override
    public String name() {
        return channel.name();
    }

    @Override
    public long size() {
        return channel.size();
    }


    public static class Writer implements Closeable {

        private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> Buffers.allocate(Memory.PAGE_SIZE, false));

        private final SegmentChannel channel;
        private final BloomFilter filter;
        private final ByteBuffer buffer = Buffers.allocate(IndexKey.BYTES, false);
        private long entries;

        Writer(File file, int expectedEntries, double bpFpPercentage) {
            this.channel = SegmentChannel.create(file);
            this.filter = BloomFilter.create(expectedEntries, bpFpPercentage);
            writeBuffer.get().clear();
        }

        //Adds to filter and increment entries count
        //index is sparse so we need to increment here
        public void stampEntry(long stream, int version) {
            filter.add(buffer
                    .clear()
                    .putLong(stream)
                    .putInt(version)
                    .flip());

            entries++;
        }

        public void add(long stream, int version, int recordSize, int recordEntries, long logPos) {
            var data = writeBuffer.get();
            if (data.remaining() < IndexEntry.BYTES) {
                flush();
            }
            data.putLong(stream);
            data.putInt(version);
            data.putInt(recordSize);
            data.putInt(recordEntries);
            data.putLong(logPos);
        }

        public void flush() {
            var data = writeBuffer.get();
            data.flip();
            assert data.remaining() % IndexEntry.BYTES == 0;
            channel.append(data);
            data.clear();
        }

        @Override
        public void close() {
            flush();

            long indexSize = channel.size();
            checkChannelSize(channel, indexSize);

            //filter
            filter.writeTo(channel);

            //index footer
            channel.append(writeBuffer.get()
                    .clear()
                    .putLong(indexSize)
                    .putLong(entries)
                    .flip());

            channel.flush();
            channel.truncate();
            channel.close();
        }
    }

}
