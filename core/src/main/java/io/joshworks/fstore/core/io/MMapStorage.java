package io.joshworks.fstore.core.io;


import io.joshworks.fstore.core.util.MappedByteBuffers;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class MMapStorage extends RafStorage {

    private static final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
    private final int bufferSize;
    private MappedByteBuffer[] buffers;
    private final AtomicBoolean closed = new AtomicBoolean();

    private final Object LOCK = new Object();

    public MMapStorage(File file, RandomAccessFile raf) {
        super(file, raf);
        try {
            long fileLength = raf.length();
            this.bufferSize = getBufferSize(fileLength);
            int totalBuffers = getTotalBuffers(fileLength, this.bufferSize);
            this.buffers = new MappedByteBuffer[totalBuffers];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getBufferSize(long fileLength) {
        if (fileLength <= Integer.MAX_VALUE) {
            return (int) fileLength;
        }
        return Integer.MAX_VALUE;
    }

    private static int getTotalBuffers(long fileLength, int bufferSize) {
        int numFullBuffers = (int) (fileLength / bufferSize);
        long diff = fileLength % bufferSize;
        return diff == 0 ? numFullBuffers : numFullBuffers + 1;
    }

    protected int writeDirect(ByteBuffer src) {
        return super.write(src);
    }

    @Override
    public int write(ByteBuffer src) {
        Storage.ensureNonEmpty(src);

        int dataSize = src.remaining();
        int written = 0;
        long currentPosition = this.position.get();
        while (src.hasRemaining()) {
            int idx = bufferIdx(currentPosition);
            MappedByteBuffer dst = getOrAllocate(idx);

            int dstRemaining = dst.remaining();
            if (dstRemaining < dataSize) {
                int srcLimit = src.limit();
                src.limit(src.position() + dstRemaining);
                dst.put(src);
                dst.flip();
                src.limit(srcLimit);
                written += dstRemaining;
                currentPosition += dstRemaining;
            } else {
                int srcRemaining = src.remaining();
                dst.put(src);
                written += srcRemaining;
            }
        }

        position.addAndGet(written);
        return written;
    }


    @Override
    public int read(long readPos, ByteBuffer dst) {
        long writePosition = super.position.get();

        if (readPos >= writePosition) {
            return EOF;
        }

        int idx = bufferIdx(readPos);
        int bufferAddress = posOnBuffer(readPos);
        MappedByteBuffer buffer = getOrAllocate(idx);

        int srcCapacity = buffer.capacity();
        if (bufferAddress > srcCapacity) {
            throw new IllegalArgumentException("Invalid position " + readPos + ", buffer idx " + idx + ", buffer capacity " + srcCapacity);
        }

        ByteBuffer src = buffer.asReadOnlyBuffer();
        src.clear();
        src.position(bufferAddress);

        //TODO limit based on position ?
        int dstRemaining = dst.remaining();
        int srcRemaining = src.remaining();
        if (dstRemaining > srcRemaining) {
            //here
            dst.put(src);
            if (idx + 1 >= buffers.length) { //no more buffers
                return srcRemaining;
            }
            int read = read(readPos + srcRemaining, dst);
            return srcRemaining + (read >= 0 ? read : 0);
        }

        int available = (int) Math.min(readPos + dstRemaining, writePosition - readPos);
        int toBeCopied = Math.min(available, dstRemaining);
        src.limit(bufferAddress + toBeCopied);

        dst.put(src);
        return dstRemaining;
    }

    protected MappedByteBuffer getOrAllocate(int idx) {
        if (closed.get()) {
            throw new StorageException("Storage closed: " + name());
        }
        if (idx >= buffers.length) {
            synchronized (LOCK) {
                if (idx >= buffers.length && !closed.get()) {
                    growBuffersToAccommodateIdx(idx);
                }
            }
        }
        MappedByteBuffer current = buffers[idx];
        if (buffers[idx] == null) {
            synchronized (LOCK) {
                if (buffers[idx] == null && !closed.get()) {
                    buffers[idx] = map(idx);
                }
                current = buffers[idx];
            }
        }
        if (closed.get()) {
            throw new StorageException("Storage closed");
        }
        if (current == null) {
            throw new StorageException("Could not acquire buffer page");
        }
        return current;
    }

    private void growBuffersToAccommodateIdx(int newNumBuffers) {
        MappedByteBuffer[] copy = new MappedByteBuffer[newNumBuffers + 1]; //idx + 1 = number of required buffers
        System.arraycopy(buffers, 0, copy, 0, buffers.length);
        buffers = copy;
    }

    @Override
    public void position(long pos) {
        int idx = bufferIdx(pos);
        MappedByteBuffer buffer = getOrAllocate(idx);
        int bufferAddress = posOnBuffer(pos);
        buffer.position(bufferAddress);
        this.position.set(pos);
    }

    protected int posOnBuffer(long pos) {
        return (int) (pos % bufferSize);
    }

    protected int bufferIdx(long pos) {
        return (int) (pos / bufferSize);
    }

    private MappedByteBuffer map(int idx) {
        long from = ((long) idx) * bufferSize;
        try {
            return raf.getChannel().map(FileChannel.MapMode.READ_WRITE, from, bufferSize);
        } catch (Exception e) {
            close();
            throw new StorageException(e);
        }
    }


    @Override
    public void delete() {
        synchronized (LOCK) {
            this.close();
            super.delete();
        }
    }

    @Override
    public void close() {
        synchronized (LOCK) {
            closed.set(true);
            unmapAll();
            super.close();
        }
    }

    private void unmapAll() {
        for (int i = 0; i < buffers.length; i++) {
            MappedByteBuffer buffer = buffers[i];
            if (buffer != null) {
                buffer.force();
                unmap(buffer);
                buffers[i] = null;
            }
        }
    }

    private void unmap(MappedByteBuffer buffer) {
        try {
            MappedByteBuffers.unmap(buffer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void flush() {
        int idx = bufferIdx(this.position.get());
        if (idx >= buffers.length) {
            return;
        }
        MappedByteBuffer buffer = buffers[idx];
        if (buffer != null && !isWindows) {
            //caused by https://bugs.openjdk.java.net/browse/JDK-6539707
            buffer.force();
        }
    }
}