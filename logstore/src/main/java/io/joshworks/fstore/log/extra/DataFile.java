package io.joshworks.fstore.log.extra;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.RecordEntry;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

public class DataFile<T> implements Flushable, Closeable {

    private final DataStream stream;
    private final Storage storage;
    private final Serializer<T> serializer;

    private DataFile(File handler, Serializer<T> serializer, boolean mmap, long initialSize, int maxEntrySize) {
        Storage storage = null;
        try {
            this.storage = storage = Storage.createOrOpen(handler, mmap ? StorageMode.MMAP : StorageMode.RAF, initialSize);
            this.stream = new DataStream(new ThreadLocalBufferPool(maxEntrySize, false), storage, 1, Memory.PAGE_SIZE * 4);
            this.serializer = serializer;
        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw new RuntimeException(e);
        }
    }

    public static <T> Builder<T> of(Serializer<T> serializer) {
        return new Builder<>(serializer);
    }

    public long add(T record) {
        long pos = stream.write(record, serializer);
        if (Storage.EOF == pos) {
            throw new RuntimeException("Not space left in the data file");
        }
        return pos;
    }

    public long length() {
        return storage.length();
    }

    public T get(long position) {
        return stream.read(Direction.FORWARD, position, serializer).entry();
    }

    public Iterator<T> iterator(Direction direction) {
        long pos = Direction.FORWARD.equals(direction) ? 0 : stream.position();
        return iterator(direction, pos);
    }

    public Iterator<T> iterator(Direction direction, long position) {
        position = Math.min(position, stream.position());
        return new DataFileIterator<>(stream, serializer, direction, position);
    }

    @Override
    public void flush() {
        storage.flush(false);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(storage);
    }

    public void delete() {
        storage.delete();
    }

    private static class DataFileIterator<T> implements Iterator<T> {

        private final DataStream stream;
        private final Serializer<T> serializer;
        private final Queue<RecordEntry<T>> entries = new ArrayDeque<>();
        private final Direction direction;
        private long position;

        private DataFileIterator(DataStream stream, Serializer<T> serializer, Direction direction, long position) {
            this.stream = stream;
            this.serializer = serializer;
            this.direction = direction;
            this.position = position;
        }

        private void fetchEntries() {
            if (position >= stream.position()) {
                return;
            }
            List<RecordEntry<T>> read = stream.bulkRead(direction, position, serializer);
            entries.addAll(read);
        }

        @Override
        public boolean hasNext() {
            if (entries.isEmpty()) {
                fetchEntries();
            }
            return !entries.isEmpty();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                return null;
            }
            RecordEntry<T> record = entries.poll();
            position += record.recordSize();
            return record.entry();
        }
    }

    public static final class Builder<T> {

        private final Serializer<T> serializer;
        private boolean mmap;
        private long size = Size.MB.of(50);
        private int maxEntrySize = Size.MB.ofInt(1);

        private Builder(Serializer<T> serializer) {
            this.serializer = serializer;
        }

        public Builder<T> mmap() {
            this.mmap = true;
            return this;
        }

        public Builder<T> initialSize(long size) {
            this.size = size;
            return this;
        }

        public Builder<T> maxEntrySize(int maxEntrySize) {
            this.maxEntrySize = maxEntrySize;
            return this;
        }

        public DataFile<T> open(File file) {
            return new DataFile<>(file, serializer, mmap, size, maxEntrySize);
        }
    }
}
