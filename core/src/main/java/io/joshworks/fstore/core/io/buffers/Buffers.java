package io.joshworks.fstore.core.io.buffers;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;

public class Buffers {

    public static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;

    public static ByteBuffer allocate(int size, boolean direct) {
        if (size >= MAX_CAPACITY) {
            throw new IllegalArgumentException("Buffer too large: Max allowed size is: " + MAX_CAPACITY);
        }
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    public static int absolutePut(ByteBuffer src, int srcPos, ByteBuffer dst) {
        if (srcPos > src.position()) {
            return 0;
        }
        //do not use src.remaining() here as we don't expect flipped buffers
        int srcRemaining = src.position() - srcPos;
        int readableBytes = Math.min(srcRemaining, dst.remaining());
        copy(src, srcPos, readableBytes, dst);
        return readableBytes;
    }


    /**
     * Copies from the source WITHOUT using the src position / limit pointers
     * position of dst is updated with the inserted number of bytes,
     * Does not modify source's position
     *
     * @throws BufferOverflowException if the count is greater than the dst {@link ByteBuffer#remaining()}
     */
    public static int copy(ByteBuffer src, int srcStart, int count, ByteBuffer dst) {
        if (count == 0) {
            return 0;
        }
        if (srcStart < 0 || srcStart > src.capacity()) {
            throw new IndexOutOfBoundsException(srcStart);
        }
        if (count > src.capacity() - srcStart) {
            throw new IndexOutOfBoundsException("count bytes is more than available from srcStart position");
        }

        if (count > dst.remaining()) {
            throw new BufferOverflowException();
        }

        int i = 0;
        while ((count - i) >= Long.BYTES) {
            dst.putLong(src.getLong(srcStart + i));
            i += Long.BYTES;
        }
        while ((count - i) >= Integer.BYTES) {
            dst.putInt(src.getInt(srcStart + i));
            i += Integer.BYTES;
        }
        while ((count - i) >= Short.BYTES) {
            dst.putShort(src.getShort(srcStart + i));
            i += Short.BYTES;
        }
        while ((count - i) >= Byte.BYTES) {
            dst.put(src.get(srcStart + i));
            i += Byte.BYTES;
        }

        return i;
    }

    /**
     * Copies data from the src into the dst, without modifying any of the buffers positions
     */
    public static int copy(ByteBuffer src, int srcOffset, int srcCount, ByteBuffer dst, int dstOffset) {
        if (srcCount == 0) {
            return 0;
        }
        if (srcOffset < 0 || srcOffset > src.capacity()) {
            throw new IndexOutOfBoundsException(srcOffset);
        }
        if (srcCount > src.capacity() - srcOffset) {
            throw new IndexOutOfBoundsException("srcCount bytes is more than available from srcOffset position");
        }

        if (srcCount > dst.remaining()) {
            throw new BufferOverflowException();
        }

        int i = 0;
        while ((srcCount - i) >= Long.BYTES) {
            dst.putLong(dstOffset + i, src.getLong(srcOffset + i));
            i += Long.BYTES;
        }
        while ((srcCount - i) >= Integer.BYTES) {
            dst.putInt(dstOffset + i, src.getInt(srcOffset + i));
            i += Integer.BYTES;
        }
        while ((srcCount - i) >= Short.BYTES) {
            dst.putShort(dstOffset + i, src.getShort(srcOffset + i));
            i += Short.BYTES;
        }
        while ((srcCount - i) >= Byte.BYTES) {
            dst.put(dstOffset + i, src.get(srcOffset + i));
            i += Byte.BYTES;
        }

        return i;
    }

    /**
     * Copies remaining bytes from src to the dst
     * Does not modify source's position
     *
     * @throws BufferOverflowException if the src remaining bytes is greater than dst remaining bytes
     */
    public static int copy(ByteBuffer src, ByteBuffer dst) {
        if (src.remaining() > dst.remaining()) {
            throw new BufferOverflowException();
        }
        return copy(src, src.position(), src.remaining(), dst);
    }

    /**
     * Copies remaining bytes from src to the dst withou throwing exception if src has more than dst remaining bytes
     * Does not modify source's position
     */
    public static int fill(ByteBuffer src, ByteBuffer dst) {
        int min = Math.min(src.remaining(), dst.remaining());
        return copy(src, src.position(), min, dst);
    }


    /**
     * Copy as many bytes as possible from {@code sources} into {@code destination} in a "gather" fashion.
     *
     * @param destination the destination buffer
     * @param sources     the source buffers
     * @param offset      the offset into the source buffers array
     * @param length      the number of buffers to read from
     * @return the number of bytes put into the destination buffers
     */
    public static int copy(final ByteBuffer destination, final ByteBuffer[] sources, final int offset, final int length) {
        int t = 0;
        for (int i = 0; i < length; i++) {
            final ByteBuffer buffer = sources[i + offset];
            final int rem = buffer.remaining();
            if (rem == 0) {
                continue;
            } else if (rem > destination.remaining()) {
                t += destination.remaining();
                copy(buffer, buffer.position(), destination.remaining(), destination);
//                destination.put(slice(buffer, destination.remaining()));
                return t;
            } else {
                destination.put(buffer);
                t += rem;
            }
        }
        return t;
    }

    /**
     * Copy as many bytes as possible from {@code sources} into {@code destinations} in a "scatter" fashion.
     *
     * @param destinations the destination buffers
     * @param offset       the offset into the destination buffers array
     * @param length       the number of buffers to update
     * @param source       the source buffer
     * @return the number of bytes put into the destination buffers
     */
    public static int copy(final ByteBuffer[] destinations, final int offset, final int length, final ByteBuffer source) {
        int t = 0;
        for (int i = 0; i < length; i++) {
            final ByteBuffer dst = destinations[i + offset];
            final int rem = dst.remaining();
            if (rem == 0) {
                continue;
            } else if (rem < source.remaining()) {
                copy(source, source.position(), dst.remaining(), dst);
//                buffer.put(slice(source, rem));
                t += rem;
            } else {
                t += source.remaining();
                dst.put(source);
                return t;
            }
        }
        return t;
    }

    /**
     * Offset this buffer position in relation to the current position
     */
    public static void offsetPosition(ByteBuffer buffer, int offset) {
        buffer.position(buffer.position() + (offset));
    }

    /**
     * Offset this buffer limit in relation to the current position
     */
    public static void offsetLimit(ByteBuffer buffer, int offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be greater or equals to zero");
        }
        buffer.limit(buffer.position() + offset);
    }

    /**
     * Updates this buffer to the given position and its limit to a relative position from the given position
     */
    public static void view(ByteBuffer buffer, int offset, int count) {
        buffer.limit(offset + count).position(offset);
    }

    /**
     * The absolute position in the backing array, considering the array offset of the HeapByteBuffer,
     * For OffHeapBuffers it simply return the {@link ByteBuffer#position()}
     */
    public static int absoluteArrayPosition(ByteBuffer buffer) {
        return absoluteArrayPosition(buffer, buffer.position());
    }

    /**
     * The absolute position in the backing array, considering the array offset of the HeapByteBuffer,
     * For OffHeapBuffers it simply return the position
     */
    public static int absoluteArrayPosition(ByteBuffer buffer, int position) {
        if (position < 0) {
            throw new IndexOutOfBoundsException(position);
        }
        if (buffer.hasArray()) {
            return position + buffer.arrayOffset();
        }

        return position;
    }

    public static byte[] copyArray(ByteBuffer bb) {
        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        return data;
    }

    /**
     * Determine whether any of the buffers has remaining data.
     *
     * @param buffers the buffers
     * @param offs    the offset into the buffers array
     * @param len     the number of buffers to check
     * @return {@code true} if any of the selected buffers has remaining data
     */
    public static boolean hasRemaining(final Buffer[] buffers, final int offs, final int len) {
        for (int i = 0; i < len; i++) {
            if (buffers[i + offs].hasRemaining()) {
                return true;
            }
        }
        return false;
    }


    public static int toAbsolutePosition(ByteBuffer buffer, int relativeOffset) {
        return buffer.position() + relativeOffset;
    }

    public static int relativeRemaining(ByteBuffer buffer, int offset) {
        return buffer.limit() - (buffer.position() + offset);
    }

    public static int remaining(ByteBuffer buffer, int absPos) {
        return buffer.limit() - absPos;
    }

    public static long remaining(ByteBuffer[] buffers, int offset, int count) {
        long remaining = 0;
        for (int i = 0; i < count; i++) {
            remaining += buffers[offset + i].remaining();
        }
        return remaining;
    }

    public static int writeFully(WritableByteChannel dst, ByteBuffer buffer) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            total += dst.write(buffer);
        }
        return total;
    }

    public static long writeFully(GatheringByteChannel dst, ByteBuffer[] buffers, int offset, int count) throws IOException {
        long remaining = remaining(buffers, offset, count);
        if (remaining == 0) {
            return 0;
        }

        long written = 0;
        while (written < remaining) {
            long w = dst.write(buffers, offset, count);
            if (w == -1) {
                return -1;
            }
            written += w;
        }
        return written;
    }

    public static ByteBuffer wrap(long l) {
        return ByteBuffer.allocate(Long.BYTES).putLong(l).flip();
    }

    public static ByteBuffer wrap(int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).flip();
    }

    public static ByteBuffer wrap(double d) {
        return ByteBuffer.allocate(Double.BYTES).putDouble(d).flip();
    }

    public static ByteBuffer wrap(short s) {
        return ByteBuffer.allocate(Short.BYTES).putShort(s).flip();
    }

}
