package io.joshworks.fstore.core.io;


import io.joshworks.fstore.core.util.MappedByteBuffers;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MMapStorage2 extends MemoryStorage {

    private final RandomAccessFile raf;
    private final Storage delegate;
    private final boolean isWindows;


    public MMapStorage2(File file, RandomAccessFile raf) {
        super(fileLength(raf));
        this.delegate = new RafStorage(file, raf);
        this.raf = raf;
        isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
    }

    private static long fileLength(RandomAccessFile raf) {
        try {
            return raf.length();
        } catch (IOException e) {
            IOUtils.closeQuietly(raf);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ByteBuffer create(int bufferSize) {
        return map(position(), bufferSize);
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
    public long length() {
        return 0;
    }


    @Override
    public long position() {
        return position;
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
        delegate.delete();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void close() {
        try {
            unmapAll();
            delegate.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void unmapAll() {
        for (int i = 0; i < buffers.length; i++) {
            MappedByteBuffer buffer = (MappedByteBuffer) buffers[i];
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
        int idx = bufferIdx(this.position);
        if (idx > buffers.length) {
            return;
        }
        MappedByteBuffer buffer = (MappedByteBuffer) buffers[idx];
        if (buffer != null && !isWindows) {
            //caused by https://bugs.openjdk.java.net/browse/JDK-6539707
            buffer.force();
        }
    }
}