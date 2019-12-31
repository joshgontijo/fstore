package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.serializer.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IndexedSegment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    private final File file;
    private final FileChannel channel;
    private final SparseIndex<Long> offsetIndex;
    private final SparseIndex<Long> timestampIndex;

    private final AtomicLong writePosition = new AtomicLong();
    private final AtomicBoolean readOnly = new AtomicBoolean();

    private final static int INDEX_SPARSENESS = 4096;

    public IndexedSegment(File file, int maxIndexSize, boolean readOnly) throws IOException {
        this.file = file;
        this.readOnly.set(readOnly);
        boolean newFile = FileUtils.createIfNotExists(file);
        if (newFile && readOnly) {
            throw new IllegalArgumentException("Read only segment must have a existing file");
        }
        this.channel = openChannel(file, readOnly);
        if (newFile) {
            this.channel.force(true);
        }
        boolean headReopened = !newFile && !readOnly;
        String name = getFileName(file);
        File dir = file.toPath().getParent().toFile();
        File idxFile = new File(dir, name + ".offset");
        File tsFile = new File(dir, name + ".timestamp");
        if (headReopened) {
            Files.deleteIfExists(idxFile.toPath());
            Files.deleteIfExists(tsFile.toPath());
        }
        this.offsetIndex = new SparseIndex<>(idxFile, maxIndexSize, Long.BYTES, INDEX_SPARSENESS, Serializers.LONG);
        this.timestampIndex = new SparseIndex<>(tsFile, maxIndexSize, Long.BYTES, INDEX_SPARSENESS, Serializers.LONG);

        if (headReopened) {
            restore();
        }
    }

    private FileChannel openChannel(File file, boolean readOnly) throws IOException {
        if (readOnly) {
            return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ);
        }
        return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private void restore() {
        log.info("Restoring segment {}", file.getAbsolutePath());
        long start = System.currentTimeMillis();
        RecordIterator it = new RecordIterator(channel, 0, 0, writePosition);
        while (it.hasNext()) {
            long position = it.position();
            Record record = it.next();
            tryAddToIndexes(record.offset, record.timestamp, position);
        }
        log.info("Restoring of {} completed in {}ms", file.getAbsolutePath(), System.currentTimeMillis() - start);
    }

    private String getFileName(File file) {
        return file.getName().split("\\.")[0];
    }

    //TODO try removing the writeBuffer, and using the Record internal, ideally it would be constructed with a single buffer
    void append(Record record, ByteBuffer writeBuffer) {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }
        int written = record.appendTo(channel, writeBuffer);
        long position = writePosition.getAndAdd(written);

        tryAddToIndexes(record.offset, record.timestamp, position);
    }

    private void tryAddToIndexes(long offset, long timestamp, long position) {
        offsetIndex.add(offset, position);
        timestampIndex.add(timestamp, position);
    }

    public Record read(long offset) {
        RecordIterator it = iterator(offset);
        return it.hasNext() ? it.next() : null;
    }

    public RecordIterator iterator(long startOffset) {
        IndexEntry<Long> ie = offsetIndex.floor(startOffset);
        if (ie == null) {
            return null;
        }
        long startPos = ie.logPosition;
        return new RecordIterator(channel, startOffset, startPos, writePosition);
    }

    public boolean readOnly() {
        return readOnly.get();
    }

    public boolean isFull() {
        return offsetIndex.isFull() || timestampIndex.isFull();
    }

    public void roll() {
        if (!readOnly.compareAndSet(false, true)) {
            throw new IllegalStateException("Already read only");
        }
    }

    public void flush() {
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to flush", e);
        }
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
