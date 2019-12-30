package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Consumer;

public class IndexedSegment<T> implements Log<T> {

    private final Segment<LogEntry<T>> delegate;
    private final SparseIndex<Long> offsetIndex;
    private final SparseIndex<Long> timestampIndex;
    private final Serializer<T> serializer;
    private long offset;

    private final static int INDEX_SPARSENESS = 4096;
    private long lastIndexWrite;
    private IndexEntry<Long> lastOffset;
    private IndexEntry<Long> lastTimestamp;

    public IndexedSegment(
            File file,
            StorageMode storageMode,
            long segmentDataSize,
            Serializer<T> serializer,
            BufferPool bufferPool,
            WriteMode writeMode,
            double checksumProb,
            int maxIndexSize,
            long initialSequence) {

        this.serializer = serializer;
        this.offset = initialSequence;
        String name = getFileName(file.getName());
        File idxFile = new File(name + ".idx");
        File tsFile = new File(name + ".tsi");

        boolean alreadyExists = Files.exists(file.toPath());
        this.delegate = new Segment<>(file, storageMode, segmentDataSize, new LogEntrySerializer<>(serializer), bufferPool, writeMode, checksumProb);

        try {
            if (alreadyExists) {
                Files.deleteIfExists(idxFile.toPath());
                Files.deleteIfExists(tsFile.toPath());
                reindex();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.offsetIndex = new SparseIndex<>(idxFile, maxIndexSize, Long.BYTES, Serializers.LONG);
        this.timestampIndex = new SparseIndex<>(tsFile, maxIndexSize, Long.BYTES, Serializers.LONG);
    }

    //redundant
    private void reindex() {
        try (SegmentIterator<LogEntry<T>> it = delegate.iterator(Direction.FORWARD)) {
            while (it.hasNext()) {
                long pos = it.position();
                LogEntry<T> entry = it.next();
                addToIndex(entry.offset, entry.timestamp, pos);
                offset = entry.offset;
            }
        }
    }

    private String getFileName(String name) {
        return name.split("\\.")[0];
    }

    @Override
    public long append(T data) {
        long position = delegate.append(new LogEntry<>());

        addToIndex(offset++, System.currentTimeMillis(), position);
        return offset;
    }

    private void addToIndex(long offset, long timestamp, long position) {
        if(position == Log.START || position - lastIndexWrite >= INDEX_SPARSENESS) {
            offsetIndex.write(offset, position);
            timestampIndex.write(timestamp, position);
            lastIndexWrite = position;
        }
    }

    public LogEntry<T> readEntry(long offset) {
        IndexEntry<Long> ie = offsetIndex.get(offset);
        if (ie == null) {
            return null;
        }

        for (RecordEntry<LogEntry<T>> entry : delegate.read(ie.value)) {
            if(entry.entry().offset == offset) {
                return entry.entry();
            }
        }
        return null;
    }

    @Override
    public T get(long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long physicalSize() {
        return delegate.physicalSize();
    }

    @Override
    public long logicalSize() {
        return delegate.logicalSize();
    }

    @Override
    public long dataSize() {
        return delegate.dataSize();
    }

    @Override
    public long actualDataSize() {
        return delegate.actualDataSize();
    }

    @Override
    public long uncompressedDataSize() {
        return delegate.uncompressedDataSize();
    }

    @Override
    public long headerSize() {
        return delegate.headerSize();
    }

    @Override
    public long footerSize() {
        return delegate.footerSize();
    }

    public long dataWritten() {
        return delegate.dataWritten();
    }

    @Override
    public long remaining() {
        return delegate.remaining();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public SegmentIterator<T> iterator(long position, Direction direction) {
        return delegate.iterator(position, direction);
    }

    @Override
    public SegmentIterator<T> iterator(Direction direction) {
        return delegate.iterator(direction);
    }

    @Override
    public long position() {
        return delegate.position();
    }

    public void read(long position, ByteBuffer buffer) {
        delegate.read(position, buffer);
    }

    @Override
    public void delete() {
        delegate.delete();
        offsetIndex.delete();
        timestampIndex.delete();
    }

    public void roll(int level, boolean trim, Consumer<FooterWriter> footerWriter) {
        delegate.roll(level, trim, footerWriter);
    }

    @Override
    public void roll(int level, boolean trim) {
        delegate.roll(level, trim);
    }

    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public boolean closed() {
        return delegate.closed();
    }

    @Override
    public long entries() {
        return delegate.entries();
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public long uncompressedSize() {
        return delegate.uncompressedSize();
    }

    @Override
    public Type type() {
        return delegate.type();
    }

    @Override
    public Metrics metrics() {
        return delegate.metrics();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }


    public FooterReader footerReader() {
        return delegate.footerReader();
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static class LogEntrySerializer<T> implements Serializer<LogEntry<T>> {

        private final Serializer<T> serializer;

        private LogEntrySerializer(Serializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void writeTo(LogEntry<T> data, ByteBuffer dst) {

        }

        @Override
        public LogEntry<T> fromBytes(ByteBuffer buffer) {
            return null;
        }
    }

}
