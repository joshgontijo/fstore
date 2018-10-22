package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.MappedByteBuffers;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MMapCache extends RafStorage {

    private MappedByteBuffer[] buffers;
    private final int bufferSize;

    MMapCache(File target, RandomAccessFile raf) {
        super(target, raf);
        try {
            long length = raf.length();
            int numBuffers = getTotalBuffers(length, Integer.MAX_VALUE);
            this.buffers = new MappedByteBuffer[numBuffers];
            this.bufferSize = (int) Math.min(length, Integer.MAX_VALUE);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int read(long position, ByteBuffer dst) {
        int idx = bufferIdx(position);
        int bufferAddress = posOnBuffer(position);
        if (idx >= buffers.length) {
            return -1;
        }

        ByteBuffer buffer = getOrAllocate(idx, false);

        int srcCapacity = buffer.capacity();
        if (bufferAddress > srcCapacity) {
            throw new IllegalArgumentException("Invalid position " + position + ", buffer idx " + idx + ", buffer capacity " + srcCapacity);
        }

        ByteBuffer src = buffer.asReadOnlyBuffer();
        src.clear();
        src.position(bufferAddress);

        int dstRemaining = dst.remaining();
        int srcRemaining = src.remaining();
        if (dstRemaining > srcRemaining) {
            dst.put(src);
            if (idx + 1 >= buffers.length) { //no more buffers
                return srcRemaining;
            }
            int read = read(position + srcRemaining, dst);
            return srcRemaining + (read >= 0 ? read : 0);
        }

        src.limit(bufferAddress + dst.remaining());
        dst.put(src);
        return dstRemaining;
    }

    private ByteBuffer getOrAllocate(int idx, boolean expandBuffers) {
        if (idx >= buffers.length) {
            if (expandBuffers) {
                growBuffersToAccommodateIdx(idx);
            } else {
                throw new IllegalStateException("Invalid buffer index " + idx + " buffers length: " + buffers.length);
            }
        }
        ByteBuffer current = buffers[idx];
        if (current == null) {
            buffers[idx] = map(idx);
            current = buffers[idx];
        }
        return current;
    }

    private void growBuffersToAccommodateIdx(int newNumBuffers) {
        MappedByteBuffer[] copy = new MappedByteBuffer[newNumBuffers + 1]; //idx + 1 = number of required buffers
        System.arraycopy(buffers, 0, copy, 0, buffers.length);
        buffers = copy;
    }

    private int posOnBuffer(long pos) {
        return (int) (pos % bufferSize);
    }

    private int bufferIdx(long pos) {
        return (int) (pos / bufferSize);
    }

    private MappedByteBuffer map(int idx) {
        long from = ((long) idx) * bufferSize;
        return map(from, bufferSize);
    }

    private MappedByteBuffer map(long from, long size) {
        try {
            return raf.getChannel().map(FileChannel.MapMode.READ_ONLY, from, size);
        } catch (Exception e) {
            close();
            throw new StorageException(e);
        }
    }

    private static int getTotalBuffers(long fileLength, int bufferSize) {
        int numFullBuffers = (int) (fileLength / bufferSize);
        long diff = fileLength % bufferSize;
        return diff == 0 ? numFullBuffers : numFullBuffers + 1;
    }

    private void unmapAll() {
        for (int i = 0; i < buffers.length; i++) {
            MappedByteBuffer buffer = buffers[i];
            if(buffer != null) {
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
    public void delete() {
        unmapAll();
        super.delete();
    }

    @Override
    public void close() {
        unmapAll();
        super.close();
    }


    @Override
    public void truncate(long newPos) {
        int idx = bufferIdx(newPos);
        int bPos = posOnBuffer(newPos);
        for (int i = idx + 1; i < buffers.length; i++) {
            ByteBuffer buffer = buffers[i];
            if (buffer != null) {
                buffers[i] = null;
            }
        }
        ByteBuffer current = getOrAllocate(idx, true);
        current.clear().limit(bPos);
    }

    @Override
    public void extend(long newLength) {
        if(newLength < length()) {
            return;
        }
        super.extend(newLength);

        int totalBuffers = getTotalBuffers(newLength, this.bufferSize);
        if(totalBuffers > buffers.length) {
            growBuffersToAccommodateIdx(totalBuffers);
        }
    }

}