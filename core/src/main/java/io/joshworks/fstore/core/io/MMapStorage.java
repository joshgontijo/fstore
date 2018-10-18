package io.joshworks.fstore.core.io;


import io.joshworks.fstore.core.util.MappedByteBuffers;
import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MMapStorage extends DiskStorage {

    private final int bufferSize;
    MappedByteBuffer[] buffers;

    private final boolean isWindows;

    public MMapStorage(File file, RandomAccessFile raf, int bufferSize) {
        super(file, raf);
        this.bufferSize = getBufferSize(bufferSize);
        isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
        try {
            long fileLength = raf.length();
            int totalBuffers = getTotalBuffers(fileLength, this.bufferSize);
            this.buffers = new MappedByteBuffer[totalBuffers];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    private static long alignWithBuffer(long fileSize, int bufferSize) {
//        int alignedBufferSize = getBufferSize(bufferSize);
//        if(fileSize % alignedBufferSize > 0) {
//            long fullPages = fileSize / alignedBufferSize;
//            return fullPages + alignedBufferSize;
//        }
//        return fileSize;
//    }

    private static int getBufferSize(int size) {
        if(size % Memory.PAGE_SIZE > 0) {
            int fullPages = size / Memory.PAGE_SIZE;
            return fullPages + Memory.PAGE_SIZE;
        }
        return size;
    }

    private static int getTotalBuffers(long fileLength, int bufferSize) {
        int numFullBuffers = (int) (fileLength / bufferSize);
        long diff = fileLength % bufferSize;
        return diff == 0 ? numFullBuffers : numFullBuffers + 1;
    }


    @Override
    public int write(ByteBuffer src) {
        Storage.ensureNonEmpty(src);

        int dataSize = src.remaining();

        int idx = bufferIdx(this.position);
        MappedByteBuffer dst = getOrAllocate(idx, true);

        int dstRemaining = dst.remaining();
        if (dstRemaining < dataSize) {
            int srcLimit = src.limit();
            src.limit(src.position() + dstRemaining);
            dst.put(src);
            dst.flip();
            src.limit(srcLimit);
            position += dstRemaining;
            return write(src) + dstRemaining;
        }

        int srcRemaining = src.remaining();
        dst.put(src);
        position += srcRemaining;
        return srcRemaining;
    }

    @Override
    public int read(long position, ByteBuffer dst) {
        int idx = bufferIdx(position);
        int bufferAddress = posOnBuffer(position);
        if (idx >= buffers.length) {
            return -1;
        }

        MappedByteBuffer buffer = getOrAllocate(idx, false);

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

    private MappedByteBuffer getOrAllocate(int idx, boolean expandBuffers) {
        if (idx >= buffers.length) {
            if (expandBuffers) {
                growBuffersToAccommodateIdx(idx);
            } else {
                throw new IllegalStateException("Invalid buffer index " + idx + " buffers length: " + buffers.length);
            }
        }
        MappedByteBuffer current = buffers[idx];
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

    @Override
    public void position(long pos) {
        int idx = bufferIdx(position);
        MappedByteBuffer buffer = getOrAllocate(idx, true);
        int bufferAddress = posOnBuffer(pos);
        buffer.position(bufferAddress);
        this.position = pos;
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
            return raf.getChannel().map(FileChannel.MapMode.READ_WRITE, from, size);
        } catch (Exception e) {
            close();
            throw new StorageException(e);
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

    private void unmapAll() {
        for (int i = 0; i < buffers.length; i++) {
            MappedByteBuffer buffer = buffers[i];
            if(buffer != null) {
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
    public void truncate(long newPos) {
        int idx = bufferIdx(newPos);
        int bPos = posOnBuffer(newPos);
        for (int i = idx + 1; i < buffers.length; i++) {
            MappedByteBuffer buffer = buffers[i];
            if (buffer != null) {
                unmap(buffer);
                buffers[i] = null;
            }
        }
        MappedByteBuffer current = getOrAllocate(idx, true);
        current.clear().position(bPos);

//        super.truncate(newPos); //TODO might fail because of unreleased buffers, just leave it out ?
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

    @Override
    public void flush() {
        int idx = bufferIdx(this.position);
        if(idx > buffers.length) {
            return;
        }
        MappedByteBuffer buffer = buffers[idx];
        if (buffer != null && !isWindows) {
            //caused by https://bugs.openjdk.java.net/browse/JDK-6539707
            buffer.force();
        }
    }
}