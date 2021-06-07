package io.joshworks.es2.index;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.mmap.MappedFile;
import io.joshworks.fstore.core.util.Memory;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BIndex {

    private final MappedFile mf;

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> Buffers.allocate(Memory.PAGE_SIZE, false));

    private BIndex(MappedFile mf) {
        this.mf = mf;
    }

    public static BIndex.Writer writer(File file) {
        return new Writer(file);
    }

    public static BIndex open(File file) {
        try {
            if (!file.exists()) {
                throw new RuntimeIOException("File does not exist");
            }
            var mf = MappedFile.open(file, FileChannel.MapMode.READ_ONLY);
            long fileSize = mf.capacity();
            if (fileSize % IndexEntry.BYTES != 0) {
                throw new IllegalStateException("Invalid index file length: " + fileSize);
            }

            return new BIndex(mf);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize index", e);
        }
    }

    public IndexEntry find(long stream, int version, IndexFunction fn) {
        int idx = binarySearch(stream, version);
        idx = fn.apply(idx);
        return read(idx);
    }

    private int binarySearch(long stream, int version) {
        int entries = entries();

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

    private int entries() {
        return mf.capacity() / IndexEntry.BYTES;
    }

    public void close() {
        mf.close();
    }

    public void delete() {
        mf.delete();
    }


    public static class Writer implements Closeable {

        private final SegmentChannel channel;

        Writer(File file) {
            this.channel = SegmentChannel.create(file);
        }

        public void add(long stream, int version, int recordSize, int recordEntries, long logPos) {
            var data = BIndex.writeBuffer.get();
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
            var data = BIndex.writeBuffer.get();
            data.flip();
            channel.append(data);
            data.clear();
        }

        @Override
        public void close() {
            flush();
            channel.flush();
            channel.close();
        }
    }

}
