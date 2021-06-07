package io.joshworks.es2.index;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.mmap.MappedFile;
import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BIndex {

    private final MappedFile mf;

    private static final ThreadLocal<ByteBuffer> buffer = ThreadLocal.withInitial(() -> Buffers.allocate(Memory.PAGE_SIZE, false));

    public static BIndex.Writer writer(File file) {
        return new Writer(file);
    }

    public static BIndex open(File file, int blockSize) {
        try {
            if (!file.exists()) {
                throw new RuntimeIOException("File does not exist");
            }
            MappedFile mf = MappedFile.open(file, FileChannel.MapMode.READ_ONLY);
            long fileSize = mf.capacity();
            if (fileSize % blockSize != 0) {
                throw new IllegalStateException("Invalid index file length: " + fileSize);
            }

            return new BIndex(mf);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize index", e);
        }
    }

    private BIndex(MappedFile mf) {
        this.mf = mf;
    }

    static class Writer {

        private final SegmentChannel channel;

        Writer(File file) {
            this.channel = SegmentChannel.create(file);
        }

        public void add(long stream, int version, int recordSize, int recordEntries, long logPos) {
            var data = BIndex.buffer.get();
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
            var data = BIndex.buffer.get();
            data.clear();
            channel.append(data);
            data.clear();
        }

    }

}
