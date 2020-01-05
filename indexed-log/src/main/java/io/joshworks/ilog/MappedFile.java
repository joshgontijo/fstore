package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.MappedByteBuffers;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

public class MappedFile {

    public static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    private final File file;
    private final FileChannel channel;
    private MappedByteBuffer mbb;

    public MappedFile(File file, FileChannel channel, MappedByteBuffer mbb) {
        this.file = file;
        this.channel = channel;
        this.mbb = mbb;
    }

    public static MappedFile create(File file, int size) {
        validateSize(size);
        try {
            var raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            FileChannel channel = raf.getChannel();
            var mbb = map(channel, FileChannel.MapMode.READ_WRITE);
            return new MappedFile(file, channel, mbb);
        } catch (Exception e) {
            throw new RuntimeIOException("Could not open mapped file", e);
        }
    }

    public static MappedFile open(File file) {
        try {
            var raf = new RandomAccessFile(file, "r");
            long length = raf.length();
            validateSize(length);
            FileChannel channel = raf.getChannel();
            var mbb = map(channel, FileChannel.MapMode.READ_ONLY);
            mbb.position((int) length);
            return new MappedFile(file, channel, mbb);
        } catch (Exception e) {
            throw new RuntimeIOException("Could not open mapped file", e);
        }
    }

    public long getLong(int idx) {
        return mbb.getLong(idx);
    }

    public void write(ByteBuffer buffer) {
        mbb.put(buffer);
    }

    public void put(byte b) {
        mbb.put(b);
    }

//    //TODO this might cause JVM crash since truncate can be called or any other function that destroys the buffer
//    //INVESTIGATE HOW TO APPROACH THIS
//    public BufferReader reader(int position) {
//        return new BufferReader(mbb, position);
//    }

    public long capacity() {
        return mbb.capacity();
    }

    public long position() {
        return mbb.position();
    }

    public void delete() throws IOException {
        close();
        Files.deleteIfExists(file.toPath());
    }

    public void close() {
        MappedByteBuffers.unmap(mbb);
        IOUtils.closeQuietly(channel);
    }

    public void truncate(long newLength) {
        try {
            if (newLength == mbb.capacity()) {
                return;
            }
            validateSize(newLength);
            MappedByteBuffers.unmap(mbb);
            channel.truncate(newLength);
            mbb = map(channel, FileChannel.MapMode.READ_WRITE);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to truncate " + file.getAbsoluteFile(), e);
        }
    }

    public void putLong(long l) {
        mbb.putLong(l);
    }

    public String name() {
        return file.getName();
    }

    public void flush() {
        mbb.force();
    }

    private static MappedByteBuffer map(FileChannel channel, FileChannel.MapMode mode) throws IOException {
        long size = channel.size();
        return channel.map(mode, 0, size);
    }

    private static void validateSize(long size) {
        if (size > MAX_BUFFER_SIZE) {
            throw new IllegalArgumentException("File size must be less than " + MAX_BUFFER_SIZE);
        }
    }
}
