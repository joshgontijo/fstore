package io.joshworks.fstore.core.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.Objects;

public abstract class DiskStorage implements Storage {

    protected RandomAccessFile raf;
    protected FileChannel channel;
    protected File file;
    protected FileLock lock;
    protected long position;

    DiskStorage(File target, RandomAccessFile raf) {
        Objects.requireNonNull(target, "File must specified");
        this.raf = raf;
        try {
            this.file = target;
            this.channel = raf.getChannel();
            this.lock = this.channel.lock();

        } catch (Exception e) {
            IOUtils.closeQuietly(raf);
            IOUtils.closeQuietly(channel);
            throw new StorageException("Failed to open storage of " + target.getName(), e);
        }
    }

    @Override
    public long length() {
        try {
            return raf.length();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public void position(long position) {
        try {
            channel.position(position);
            this.position = position;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void delete() {
        try {
            close();
            Files.delete(file.toPath());
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void close() {
        flush();
        if (channel.isOpen()) {
            IOUtils.releaseLock(lock);
        }
        IOUtils.closeQuietly(channel);
        IOUtils.closeQuietly(raf);
    }

    @Override
    public void flush() {
        try {
            if (channel.isOpen()) {
                channel.force(true);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public String name() {
        return file.getName();
    }

}
