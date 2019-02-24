package io.joshworks.fstore.core.io;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public abstract class DiskStorage implements Storage {

    protected final RandomAccessFile raf;
    final FileChannel channel;
    protected final File file;
    protected final FileLock lock;
    protected final AtomicLong position = new AtomicLong();
    private final long size;

    DiskStorage(File target, RandomAccessFile raf) {
        Objects.requireNonNull(target, "File must specified");
        this.raf = raf;
        this.file = target;
        this.channel = raf.getChannel();

        try {
            this.lock = this.channel.lock();
            this.size = this.raf.length();
        } catch (Exception e) {
            IOUtils.closeQuietly(raf);
            IOUtils.closeQuietly(channel);
            throw new StorageException("Failed acquire file lock: " + target.getName(), e);
        }
    }

    @Override
    public long length() {
        return size;
    }

    public void writePosition(long position) {
        validateWriteAddress(position);
        try {
            this.channel.position(position);
            this.position.set(position);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public long writePosition() {
        return position.get();
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
