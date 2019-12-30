package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

public class IndexedSegment {

    private final File file;
    private final FileChannel channel;
    private final SparseIndex<Long> offsetIndex;
    private final SparseIndex<Long> timestampIndex;

    private final AtomicLong writePosition = new AtomicLong();
    private final AtomicLong offset = new AtomicLong();

    private final static int INDEX_SPARSENESS = 4096;
    private long lastIndexWrite;
    private IndexEntry<Long> lastOffset;
    private IndexEntry<Long> lastTimestamp;

    public IndexedSegment(File file, int maxIndexSize, boolean readOnly) throws IOException {
        this.file = file;
        boolean newFile = FileUtils.createIfNotExists(file);
        this.channel = openChannel(file, readOnly);
        boolean headReopened = !newFile && !readOnly;
        String name = getFileName(file);
        File idxFile = new File(name + ".offset");
        File tsFile = new File(name + ".timestamp");
        if (headReopened) {
            Files.deleteIfExists(idxFile.toPath());
            Files.deleteIfExists(tsFile.toPath());
        }
        this.offsetIndex = new SparseIndex<>(idxFile, maxIndexSize, Long.BYTES, Serializers.LONG);
        this.timestampIndex = new SparseIndex<>(tsFile, maxIndexSize, Long.BYTES, Serializers.LONG);

        if (headReopened) {
            reindex();
        }
    }

    private FileChannel openChannel(File file, boolean readOnly) throws IOException {
        if (readOnly) {
            return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ);
        }
        return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    //redundant
    private void reindex() {
//        try (SegmentIterator<Record<T>> it = delegate.iterator(Direction.FORWARD)) {
//            while (it.hasNext()) {
//                long pos = it.position();
//                Record<T> entry = it.next();
//                addToIndex(entry.offset, entry.timestamp, pos);
//                offset = entry.offset;
//            }
//        }
    }

    private String getFileName(File file) {
        return file.getName().split("\\.")[0];
    }

    void append(Record record, ByteBuffer writeBuffer) {
        int written = record.appendTo(channel, writeBuffer);
        long position = writePosition.getAndAdd(written);

        offsetIndex.write(record.offset, position);
        timestampIndex.write(record.timestamp, position);
    }

    private void addToIndex(long offset, long timestamp, long position) {
        if (position == Log.START || position - lastIndexWrite >= INDEX_SPARSENESS) {
            offsetIndex.write(offset, position);
            timestampIndex.write(timestamp, position);
            lastIndexWrite = position;
        }
    }

    public RecordBatch read(long startOffset, long endOffset, int maxBytes) {
        IndexEntry<Long> ie = offsetIndex.floor(startOffset);
        if (ie == null) {
            return null;
        }

       return null;
    }


    public void delete() {
        try {
            channel.close();
            Files.deleteIfExists(file.toPath());
            offsetIndex.delete();
            timestampIndex.delete();
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to delete", e);
        }
    }
}
