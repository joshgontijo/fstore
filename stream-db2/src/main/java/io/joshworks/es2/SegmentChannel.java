package io.joshworks.es2;

import io.joshworks.es2.directory.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Channels;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.MappedByteBuffers;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentChannel implements Closeable, SegmentFile {

    private final File handle;
    private final FileChannel channel;
    private final AtomicLong writePosition = new AtomicLong();
    private final FileLock lock;

    private SegmentChannel(File handle, FileChannel channel, FileLock lock) {
        this.handle = handle;
        this.channel = channel;
        this.lock = lock;
    }

    public static SegmentChannel create(File file) {
        try {
            var channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileLock lock = channel.lock();
            return new SegmentChannel(file, channel, lock); //3 = header + data + footer
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to create segment " + file.getName(), e);
        }
    }

    public static SegmentChannel create(File file, long size) {
        try {
            FileUtils.tryCreate(file);
            var raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            raf.close();
            var channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            var lock = channel.lock();

            return new SegmentChannel(file, channel, lock);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to create segment " + file.getName(), e);
        }
    }

    public static SegmentChannel open(File file, long size) {
        try {
            FileUtils.createIfNotExists(file);
            var raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            raf.close();
            var channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            var lock = channel.lock();

            return new SegmentChannel(file, channel, lock);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to create segment " + file.getName(), e);
        }
    }

    public static SegmentChannel open(File file) {
        try {
            var channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            var lock = channel.lock();
            var segment = new SegmentChannel(file, channel, lock);
            segment.position(channel.size());
            return segment;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment " + file.getName(), e);
        }
    }

    public synchronized long append(ByteBuffer src) {
        long written = Channels.writeFully(channel, src);
        long writePos = writePosition.get();
        writePosition.accumulateAndGet(written, (curr, add) -> curr + written);
        return writePos;
    }

    public synchronized long append(ByteBuffer[] srcs, int count) {
        long written = Channels.writeFully(channel, srcs, 0, count);
        long writePos = writePosition.get();
        writePosition.accumulateAndGet(written, (curr, add) -> curr + written);
        return writePos;
    }

    public int read(ByteBuffer dst, long position) {
        return Channels.read(channel, position, dst);
    }

    public long position() {
        return writePosition.get();
    }

    public void position(long newPosition) {
        try {
            writePosition.set(newPosition);
            channel.position(newPosition);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to set position", e);
        }
    }

    @Override
    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to get size", e);
        }
    }

    public synchronized void truncate() {
        try {
            long pos = writePosition.get();
            channel.truncate(pos);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to truncate file");
        }
    }

    public synchronized void resize(long size) {
        try {
            //sets the writePosition to the truncated size if greater than truncated size
            writePosition.accumulateAndGet(size, Math::min);
            channel.truncate(size);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to truncate file", e);
        }
    }

    public void flush() {
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to flush channel", e);
        }
    }

    public long transferTo(long position, long count, WritableByteChannel target) {
        return Channels.transferFully(channel, position, count, target);
    }

    public long transferFrom(ReadableByteChannel src, long position, long count) {
        try {
            return channel.transferFrom(src, position, count);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public synchronized MappedReadRegion map(long position, int size) {
        try {
            long writePos = writePosition.get();
            long endPos = position + size;
            if (position > writePos) {
                throw new IllegalArgumentException("Invalid map start address: " + position + " current writePos" + writePos);
            }
            if (endPos > writePos) {
                throw new IllegalArgumentException("Invalid map end address: " + endPos + " current writePos" + writePos);
            }
            MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, position, size);
            return new MappedReadRegion(map);

        } catch (IOException e) {
            throw new RuntimeIOException("Failed to map", e);
        }
    }

    public synchronized void delete() {
        try {
            System.err.println("Deleting " + handle.getName());
            close();
            Files.delete(handle.toPath());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public String name() {
        return handle.getName().split("\\.")[0];
    }

    @Override
    public synchronized void close() {
        try {
            lock.close();
            channel.close();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public String toString() {
        return "SegmentChannel{" +
                " name=" + name() +
                " handle=" + handle +
                " size=" + size() +
                " pos=" + position() +
                '}';
    }

    public static class MappedReadRegion implements Closeable {

        private ByteBuffer mbb;

        private MappedReadRegion(ByteBuffer mbb) {
            this.mbb = mbb;
        }

        /**
         * Copy data from this MappedFile into the destination buffer
         *
         * @param dst    The destination buffer
         * @param offset the offset of the source (this MappedFile)
         * @param count  The number of bytes to be copied
         * @return the number of bytes copied
         * @throws BufferOverflowException if the count is greater than the dst {@link ByteBuffer#remaining()}
         */
        public int get(ByteBuffer dst, int offset, int count) {
            return Buffers.copy(mbb, offset, count, dst);
        }

        public ByteBuffer slice(int index, int length) {
            return mbb.slice(index, length).asReadOnlyBuffer();
        }

        public long getLong(int idx) {
            return mbb.getLong(idx);
        }

        public int getInt(int idx) {
            return mbb.getInt(idx);
        }

        public double getDouble(int idx) {
            return mbb.getDouble(idx);
        }

        public float getFloat(int idx) {
            return mbb.getFloat(idx);
        }

        public short getShort(int idx) {
            return mbb.getShort(idx);
        }

        public byte get(int idx) {
            return mbb.get(idx);
        }

        public int capacity() {
            return mbb.capacity();
        }

        public int position() {
            return mbb.position();
        }

        public long remaining() {
            return mbb.remaining();
        }

        public void position(int position) {
            mbb.position(position);
        }

        @Override
        public synchronized void close() {
            if (mbb instanceof MappedByteBuffer tmp) {
                mbb = null;
                MappedByteBuffers.unmap(tmp);
            }
        }
    }

}
