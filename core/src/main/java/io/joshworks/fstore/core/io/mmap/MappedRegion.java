package io.joshworks.fstore.core.io.mmap;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.MappedByteBuffers;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.joshworks.fstore.core.io.buffers.Buffers.MAX_CAPACITY;

/**
 * Region mapped file, with relative put methods and absolute read methods
 */
public class MappedRegion {

    private static final byte[] ZERO = new byte[8192];
    protected final long regionStart;
    protected final FileChannel.MapMode mode;
    protected MappedByteBuffer mbb;

    public MappedRegion(FileChannel channel, long regionStart, long size, FileChannel.MapMode mode) {
        this.regionStart = regionStart;
        this.mode = mode;
        try {
            map(channel, size, mode);
        } catch (Exception e) {
            throw new RuntimeIOException("Could not open mapped file", e);
        }
    }

    static int safeCast(long size) {
        if (size > MAX_CAPACITY) {
            throw new IllegalArgumentException("File size must be less than " + MAX_CAPACITY + ", got " + size);
        }
        return (int) size;
    }

    protected void map(FileChannel channel, long size, FileChannel.MapMode mode) throws IOException {
        this.mbb = channel.map(mode, regionStart, safeCast(size));
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

    /**
     * Copy data from this MappedFile into the destination buffer
     *
     * @param src    The source buffer
     * @param offset the offset of the source
     * @param count  The number of bytes to be copied
     * @return the number of bytes copied
     * @throws BufferOverflowException if the count is greater than the dst {@link ByteBuffer#remaining()}
     */
    public void put(ByteBuffer src, int offset, int count) {
        Buffers.copy(src, offset, count, mbb);
    }

    public void putLong(long l) {
        mbb.putLong(l);
    }

    public void putInt(int i) {
        mbb.putInt(i);
    }

    public void putShort(short s) {
        mbb.putShort(s);
    }

    public void putDouble(double d) {
        mbb.putDouble(d);
    }

    public void putFloat(float f) {
        mbb.putFloat(f);
    }

    public void put(byte b) {
        mbb.put(b);
    }

    public void put(ByteBuffer buffer) {
        mbb.put(buffer);
    }

    public void put(byte[] bytes) {
        mbb.put(bytes);
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

    public void position(long position) {
        mbb.position(safeCast(position));
    }

    public ByteBuffer slice() {
        return mbb.slice();
    }

    public ByteBuffer slice(int index, int length) {
        return mbb.slice(index, length);
    }

    public FileChannel.MapMode mode() {
        return mode;
    }

    /**
     * Rewrites the section with zeroes
     */
    public void clear() {
        int position = mbb.position();
        rewind();
        int overwritten = 0;
        while (overwritten < position) {
            int remaining = position - overwritten;
            int size = Math.min(remaining, ZERO.length);
            mbb.put(ZERO, 0, size);
            overwritten += size;
        }
    }

    public void rewind() {
        mbb.position(0);
    }

    public void close() {
        MappedByteBuffers.unmap(mbb);
        mbb = null;
    }

    public void flush() {
        mbb.force();
    }

    ByteBuffer buffer() {
        return mbb;
    }
}
