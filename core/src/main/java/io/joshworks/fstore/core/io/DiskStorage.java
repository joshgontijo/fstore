package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class DiskStorage implements Storage {

    protected final RandomAccessFile raf;
    final FileChannel channel;
    protected final File file;
    protected final FileLock lock;
    protected final AtomicLong position = new AtomicLong();
    protected final AtomicLong size = new AtomicLong();

    public DiskStorage(File target, long size, RandomAccessFile raf) {
        Objects.requireNonNull(target, "File must specified");
        this.raf = raf;
        this.file = target;
        this.channel = raf.getChannel();
        try {
            this.lock = this.channel.lock();
            this.size.set(size);
        } catch (Exception e) {
            IOUtils.closeQuietly(raf);
            IOUtils.closeQuietly(channel);
            throw new StorageException("Failed acquire file lock: " + target.getName(), e);
        }
    }

    /**
     * Using channel.write(buffer, position) will result in a pwrite() sys call
     */
    @Override
    public int write(ByteBuffer data) {
        Storage.ensureNonEmpty(data);
        try {
            int written = 0;
            while (data.hasRemaining()) {
                written += channel.write(data);
            }
            updatePositionAndSize(written);
            return written;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public int write(long position, ByteBuffer data) {
        Storage.ensureNonEmpty(data);
        try {
            int written = 0;
            while (data.hasRemaining()) {
                written += channel.write(data, position);
            }
            addToSize(position + written);
            return written;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public long write(ByteBuffer[] src) {
        try {
            long written = channel.write(src);
            updatePositionAndSize(written);
            return written;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private void updatePositionAndSize(long written) {
        long updatedPos = position.addAndGet(written);
        addToSize(updatedPos);
    }

    private void addToSize(long newPos) {
        size.accumulateAndGet(newPos, (curr, newVal) -> newVal > curr ? newVal : curr);
    }

    @Override
    public int read(long position, ByteBuffer dst) {
        try {
            if (!hasAvailableData(position)) {
                return EOF;
            }
            long writePosition = position();
            int read = 0;
            int totalRead = 0;
            while (dst.hasRemaining() && read >= 0) {
                long currReadPosition = position + totalRead;
                int remaining = dst.remaining();
                long available = Math.min(remaining, writePosition - currReadPosition);
                int limit = (int) Math.min(remaining, available);
                dst.limit(limit);

                read = channel.read(dst, currReadPosition);
                if (read > 0) {
                    totalRead += read;
                }
            }
            return totalRead;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public long length() {
        return size.get();
    }

    public void position(long position) {
        try {
            this.channel.position(position);
            this.position.set(position);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public long position() {
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

    @Override
    public void truncate(long newSize) {
        if (newSize >= length()) {
            return;
        }
        try {
            channel.truncate(newSize);
            size.set(newSize);
            position.accumulateAndGet(newSize, Math::min);
        } catch (Exception e) {
            throw new StorageException("Failed to truncate", e);
        }
    }

}
