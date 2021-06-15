package io.joshworks.fstore.core.io.mmap;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.MappedByteBuffers;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

public class MappedFile extends MappedRegion {

    private final File file;
    private final FileChannel channel;

    private MappedFile(FileChannel channel, File file, long size, FileChannel.MapMode mode) {
        super(channel, 0, size, mode);
        this.file = file;
        this.channel = channel;
    }

    public static MappedFile create(File file, long size, FileChannel.MapMode mode) {
        try {
            var raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            FileChannel channel = raf.getChannel();
            return new MappedFile(channel, file, size, mode);
        } catch (Exception e) {
            throw new RuntimeIOException("Could not open mapped file", e);
        }
    }

    public static MappedFile open(File file, FileChannel.MapMode mode) {
        try {
            var raf = new RandomAccessFile(file, "rw");
            long length = raf.length();
            FileChannel channel = raf.getChannel();
            var mappedFile = new MappedFile(channel, file, length, mode);
            mappedFile.position(length);
            return mappedFile;
        } catch (Exception e) {
            throw new RuntimeIOException("Could not open mapped file", e);
        }
    }

    public void delete() {
        try {
            close();
            Files.deleteIfExists(file.toPath());
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to delete " + file.getAbsolutePath(), e);
        }
    }

    public void close() {
        super.close();
        IOUtils.closeQuietly(channel);
    }

    public void truncate(int newLength) {
        try {
            if (newLength == mbb.capacity()) {
                return;
            }
            newLength = safeCast(newLength);
            MappedByteBuffers.unmap(mbb);
            channel.truncate(newLength);
            map(channel, newLength, mode);
            mbb.position(newLength);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to truncate " + file.getAbsoluteFile(), e);
        }
    }

    public String name() {
        return file.getName();
    }

    public File file() {
        return file;
    }

}
