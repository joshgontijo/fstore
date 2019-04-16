package io.joshworks.fstore.log.extra;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.MMapCache;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.GrowingThreadBufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.util.Iterator;

public class DataFile<T> implements Flushable, Closeable {

    //safe integer value
    private static final int MAX_FILE_SIZE = Integer.MAX_VALUE - 8;
    private final Log<T> segment;


    private DataFile(File handler, Serializer<T> serializer, boolean mmap, int maxEntrySize, String magic) {
        Storage storage = null;
        try {
            storage = createStorage(handler, mmap);
            DataStream dataStream = new DataStream(new GrowingThreadBufferPool(), 1.0, maxEntrySize);
            this.segment = new Segment<>(storage, serializer, dataStream, magic, Type.LOG_HEAD);
        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw new RuntimeException(e);
        }
    }

    private static Storage createStorage(File handler, boolean mmap) {
        RafStorage rafStorage = null;
        try {
            rafStorage = new RafStorage(handler, MAX_FILE_SIZE, IOUtils.randomAccessFile(handler));
            return mmap ? new MMapCache(rafStorage) : rafStorage;
        } catch (Exception e) {
            IOUtils.closeQuietly(rafStorage);
            throw new RuntimeException(e);
        }
    }

    public static <T> Builder<T> of(Serializer<T> serializer) {
        return new Builder<>(serializer);
    }

    public synchronized long add(T data) {
        long pos = segment.append(data);
        if (Storage.EOF == pos) {
            throw new RuntimeException("Not space left in the data file");
        }
        return pos;
    }

    public long length() {
        return segment.position();
    }

    public T get(long position) {
        return segment.get(position);
    }

    public LogIterator<T> iterator(Direction direction) {
        return segment.iterator(direction);
    }

    public Iterator<T> iterator(Direction direction, long position) {
        return segment.iterator(position, direction);
    }

    @Override
    public void flush() {
        segment.flush();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(segment);
    }

    public synchronized void markAsReadOnly() {
        if (!segment.readOnly()) {
            segment.roll(0);
        }
    }

    public synchronized void delete() {
        segment.delete();
    }



    public static final class Builder<T> {

        private final Serializer<T> serializer;
        private boolean mmap;
        private int maxSize = Size.MB.intOf(5);
        private String magic = "data-file-default-magic";

        public Builder(Serializer<T> serializer) {
            this.serializer = serializer;
        }

        public Builder<T> mmap() {
            this.mmap = true;
            return this;
        }

        public Builder<T> maxEntrySize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder<T> withMagic(String magic) {
            this.magic = magic;
            return this;
        }

        public DataFile<T> open(File file) {
            return new DataFile<>(file, serializer, mmap, maxSize, magic);
        }
    }
}
