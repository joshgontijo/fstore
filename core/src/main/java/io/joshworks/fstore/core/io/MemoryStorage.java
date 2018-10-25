package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public abstract class MemoryStorage implements Storage {

    private final int bufferSize;
    private final long length;
    protected ByteBuffer[] buffers;
    protected long position;

    protected abstract ByteBuffer create(int bufferSize);

    public MemoryStorage(long length) {
        this.bufferSize = getBufferSize(length);
        this.length = length;
        int totalBuffers = getTotalBuffers(length, bufferSize);
        this.buffers = new ByteBuffer[totalBuffers];
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

    @Override
    public int write(ByteBuffer src) {
        Storage.ensureNonEmpty(src);

        int dataSize = src.remaining();

        int idx = bufferIdx(this.position);
        ByteBuffer dst = getOrAllocate(idx, true);

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
            buffers[idx] = create(bufferSize);
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
        ByteBuffer buffer = getOrAllocate(idx, true);
        int bufferAddress = posOnBuffer(pos);
        buffer.position(bufferAddress);
        this.position = pos;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public long length() {
        return length;
    }

    protected int posOnBuffer(long pos) {
        return (int) (pos % bufferSize);
    }

    protected int bufferIdx(long pos) {
        return (int) (pos / bufferSize);
    }


}
