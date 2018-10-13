package io.joshworks.fstore.core.io;


import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class MMapStorage extends DiskStorage {

    private static final int NO_BUFFER = -1;

    private final int bufferSize;
    MappedByteBuffer[] buffers;
    private int writeBufferIdx;

    private final boolean isWindows;

    public MMapStorage(File file, long length, Mode mode, int bufferSize) {
        super(file, length, mode);
        this.bufferSize = bufferSize;
        isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");

        try {
            long fileLength = raf.length();
            int numFullBuffers = (int) (fileLength / bufferSize);
            long diff = fileLength % bufferSize;
            int totalBuffers = diff == 0 ? numFullBuffers : numFullBuffers + 1;
            this.buffers = new MappedByteBuffer[totalBuffers];

            if (Mode.READ_WRITE.equals(mode)) {
                MappedByteBuffer initialBuffer = map(writeBufferIdx);
                buffers[writeBufferIdx] = initialBuffer;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int write(ByteBuffer data) {
        ensureNonEmpty(data);
        checkWritable();

        int dataSize = data.remaining();

        MappedByteBuffer current = getOrAllocate(writeBufferIdx);

        int dstRemaining = current.remaining();
        if (dstRemaining < dataSize) {
            allocateNextWriteBuffer();
            int srcLimit = data.limit();
            data.limit(data.position() + dstRemaining);
            current.put(data);
            current.flip();
            data.limit(srcLimit);
            position += dstRemaining;
            return write(data) + dstRemaining;
        }

        int srcRemaining = data.remaining();
        current.put(data);
        position += srcRemaining;
        return srcRemaining;
    }

    @Override
    public int read(long position, ByteBuffer dst) {
        int idx = bufferIdx(position);
        if (idx > writeBufferIdx) {
            return -1;
        }
        if (position == 36864) {
            System.out.println();
        }
        MappedByteBuffer buffer = buffers[idx];
        if (buffer == null) {
            buffers[writeBufferIdx] = map(idx);
            buffer = buffers[idx];
        }
        int bufferAddress = posOnBuffer(position);
        if (bufferAddress > buffer.limit()) {
            return -1;
        }

        ByteBuffer src = buffer.asReadOnlyBuffer();
        if (bufferAddress > src.capacity() && idx >= writeBufferIdx) {
            throw new StorageException("Position " + position + " is out of limit (" + src.capacity() + ")");
        }

        src.position(bufferAddress);
        src.limit(src.capacity());

        int dstRemaining = dst.remaining();
        int srcRemaining = src.remaining();
        if (dstRemaining > srcRemaining) {
            dst.put(src);
            if (!readOnly() && idx == writeBufferIdx) {
                return srcRemaining;
            }
            int read = read(position + srcRemaining, dst);
            return srcRemaining + (read >= 0 ? read : 0);
        }

        src.limit(bufferAddress + dst.remaining());
        dst.put(src);
        return dstRemaining;
    }

    private MappedByteBuffer allocateNextWriteBuffer() {
        writeBufferIdx++;
        if (writeBufferIdx >= buffers.length) {
            extendBufferArray();
        }
        buffers[writeBufferIdx] = map(writeBufferIdx);
        return buffers[writeBufferIdx];
    }

    private void extendBufferArray() {
        MappedByteBuffer[] copy = new MappedByteBuffer[buffers.length + 1];
        System.arraycopy(buffers, 0, copy, 0, buffers.length);
        buffers = copy;
    }

    private MappedByteBuffer getOrAllocate(int idx) {
        MappedByteBuffer current = buffers[idx];
        if (current == null) {
            buffers[writeBufferIdx] = map(idx);
            current = buffers[idx];
        }
        return current;
    }

    @Override
    public void position(long position) {
        int idx = bufferIdx(position);
        MappedByteBuffer buffer = buffers[idx];
        if (buffer == null) {
            buffers[idx] = map(idx);
            buffer = buffers[idx];
        }
        writeBufferIdx = idx;
        int bufferAddress = posOnBuffer(position);
        buffer.position(bufferAddress);
        this.position = position;
    }

    private int posOnBuffer(long pos) {
        return (int) (pos % bufferSize);
    }

    private int bufferIdx(long pos) {
        return (int) (pos / bufferSize);
    }

    private void checkWritable() {
        if (Mode.READ.equals(mode)) {
            throw new StorageException("Storage is readonly");
        }
    }

    private MappedByteBuffer map(int idx) {
        long from = ((long) idx) * bufferSize;
        return map(from, bufferSize);
    }

    private MappedByteBuffer map(long from, long size) {
        try {
            FileChannel.MapMode mapMode = Mode.READ_WRITE.equals(mode) ? FileChannel.MapMode.READ_WRITE : FileChannel.MapMode.READ_ONLY;
            return raf.getChannel().map(mapMode, from, size);
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
        flush();
        unmapAll();
        super.close();
    }

    private boolean readOnly() {
        return Mode.READ.equals(mode);
    }

    private void unmapAll() {
        for (MappedByteBuffer buffer : buffers) {
            unmap(buffer);
        }
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = null;
        }
    }

    private void unmap(MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        try {
            flush();
            Class<?> fcClass = channel.getClass();
            Method unmapMethod = fcClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            unmapMethod.setAccessible(true);
            unmapMethod.invoke(null, buffer);

        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    //TODO just set the position to newPos and remove 'truncated' buffers ?
    @Override
    public void truncate(long pos) {
        if (Mode.READ.equals(mode)) {
            throw new StorageException("Cannot truncate read only file");
        }
//        int idx = bufferIdx(pos);
//        int bPos = posOnBuffer(pos);
//        MappedByteBuffer buffer = buffers[idx];
//        buffer.position(bPos);
//
//        int clearBufferSize = 4096;
//        if (buffer == current) {
//            while (buffer.hasRemaining()) {
//                int min = Math.min(buffer.remaining(), clearBufferSize);
//                ByteBuffer clear = ByteBuffer.allocate(clearBufferSize);
//                clear.limit(min);
//                buffer.put(clear);
//            }
//            buffer.position(bPos);
//        }
//
////        for (int i = idx + 1; i < writeBufferIdx; i++) {
////            MappedByteBuffer toUnmap = buffers.remove(i);
////            unmap(toUnmap);
////        }
////        current = buffers.get(writeBufferIdx - 1);
//        int fileSize = writeBufferIdx * bufferSize;
//        unmapAll();
//
//        super.truncate(fileSize);
//
//        current = map(0, bufferSize);
////        buffers.add(current);
//        writeBufferIdx = 0;
    }

    @Override
    public void flush() {
        MappedByteBuffer buffer = buffers[writeBufferIdx];
        if (buffer != null && Mode.READ_WRITE.equals(mode) && !isWindows) {
            //caused by https://bugs.openjdk.java.net/browse/JDK-6539707
            buffer.force();
        }
    }
}