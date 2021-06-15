package io.joshworks.es.log;

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

class SegmentChannel implements Closeable {

    private final File handle;
    private final FileChannel channel;
    private final AtomicLong writePosition = new AtomicLong();
    private final FileLock lock;

    private SegmentChannel(File handle, FileChannel channel, FileLock lock) {
        this.handle = handle;
        this.channel = channel;
        this.lock = lock;
    }

    public static SegmentChannel create(File file, long size) {
        try {
            boolean newFile = FileUtils.createIfNotExists(file);
            if (!newFile) {
                throw new RuntimeIOException("Failed already exists " + file.getAbsolutePath());
            }
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            FileChannel channel = raf.getChannel();
            FileLock lock = channel.lock();

            return new SegmentChannel(file, channel, lock);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment", e);
        }
    }

    public static SegmentChannel open(File file) {
        try {
            boolean newFile = FileUtils.createIfNotExists(file);
            FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            FileLock lock = channel.lock();
            SegmentChannel segmentChannel = new SegmentChannel(file, channel, lock);
            if (!newFile) {
                channel.position(channel.size());
            }
            return segmentChannel;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment", e);
        }
    }

    public synchronized long append(ByteBuffer src) {
        int written = Channels.writeFully(channel, src);
        long writePos = writePosition.get();
        writePosition.accumulateAndGet(written, (curr, add) -> curr + written);
        return writePos;
    }

    public int read(ByteBuffer dst, long position) {
        try {
            return channel.read(dst, position);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
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

    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    public long transferTo(long position, long count, WritableByteChannel target) {
        try {
            return channel.transferTo(position, count, target);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
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
            close();
            Files.delete(handle.toPath());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public String name() {
        return handle.getName();
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

        public MappedReadRegion slice(int index, int length) {
            ByteBuffer slice = mbb.slice(index, length);
            return new MappedReadRegion(slice);
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
