package io.joshworks.ilog;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Consumer;

public class IndexedSegment {

    private final Storage storage;
    private final SparseIndex<Long> offsetIndex;
    private final SparseIndex<Long> timestampIndex;
    private long offset;

    private final static int INDEX_SPARSENESS = 4096;
    private long lastIndexWrite;
    private IndexEntry<Long> lastOffset;
    private IndexEntry<Long> lastTimestamp;

    public IndexedSegment(File file, StorageMode storageMode, long segmentDataSize, int maxIndexSize) {
        try {
            boolean newFile = file.createNewFile();
            String name = getFileName(file.getName());
            File idxFile = new File(name + ".idx");
            File tsFile = new File(name + ".tsi");

            this.storage = Storage.create(file, storageMode);

            try {
                if (!newFile) {
                    Files.deleteIfExists(idxFile.toPath());
                    Files.deleteIfExists(tsFile.toPath());
                    reindex();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            this.offsetIndex = new SparseIndex<>(idxFile, maxIndexSize, Long.BYTES, Serializers.LONG);
            this.timestampIndex = new SparseIndex<>(tsFile, maxIndexSize, Long.BYTES, Serializers.LONG);
        } catch (Exception e) {

        }


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

    public long append(T data) {
        long position = delegate.append(new LogEntry<>());

        addToIndex(offset++, System.currentTimeMillis(), position);
        return offset;
    }

    private void addToIndex(long offset, long timestamp, long position) {
        if (position == Log.START || position - lastIndexWrite >= INDEX_SPARSENESS) {
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
            if (entry.entry().offset == offset) {
                return entry.entry();
            }
        }
        return null;
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


}
