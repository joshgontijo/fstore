package io.joshworks.ilog;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.serializer.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexedSegment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    private final File file;
    private final Storage storage;
    private final Index<Long> offsetIndex;

    private final AtomicBoolean readOnly = new AtomicBoolean();

    private final static int INDEX_SPARSENESS = 4096;

    private IndexEntry<Long> lastEntry;
    private long lastIndexWrite;

    public IndexedSegment(File file, int maxIndexSize, boolean readOnly) throws IOException {
        this.file = file;
        this.readOnly.set(readOnly);
        boolean newFile = FileUtils.createIfNotExists(file);
        if (newFile && readOnly) {
            throw new IllegalArgumentException("Read only segment must have a existing file");
        }
        this.storage = newFile ? Storage.create(file, StorageMode.RAF, Size.MB.of(100)) : Storage.open(file, StorageMode.RAF);
        if (newFile) {
            this.storage.flush(true);
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
        this.offsetIndex = new Index<>(idxFile, maxIndexSize, Long.BYTES, Serializers.LONG);

        if (headReopened) {
            restore();
        }
    }


    private void restore() {
        log.info("Restoring segment {}", file.getAbsolutePath());
        long start = System.currentTimeMillis();
        RecordIterator it = new RecordIterator(storage, 0, 0, storage::position);
        while (it.hasNext()) {
            long position = it.position();
            Record record = it.next();
            addToIndex(record.offset, position);
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

        long position = storage.position();
        record.appendTo(storage, writeBuffer);

        if (lastEntry == null) {
            addToIndex(record.offset, position);
            lastIndexWrite = position;
        } else if (position - lastIndexWrite + record.size() > INDEX_SPARSENESS) {
            addToIndex(lastEntry.key, lastEntry.logPosition);
            lastIndexWrite = position;
        }
        lastEntry = new IndexEntry<>(record.offset, position);


    }

    private void addToIndex(long offset, long position) {
        offsetIndex.write(offset, position);
    }

    public Record readSparse(long offset) {
        IndexEntry<Long> entry = offsetIndex.floor(offset);
        if (entry == null) {
            return null;
        }
        var chunk = Buffers.allocate(INDEX_SPARSENESS, false);
        storage.read(entry.logPosition, chunk);
        chunk.flip();
        while (chunk.remaining() > RecordHeader.HEADER_BYTES) {
            RecordHeader header = RecordHeader.parse(chunk);
            if (header.offset == offset) {
                if (chunk.remaining() >= header.length) {
                    return Record.from(chunk, header, true);
                }
                return null;
            }
            chunk.position(chunk.position() + header.length);
        }
        return null;
    }

    /**
     * Reads a single entry for the given offset, read is performed with a two IO calls
     * first one to read the header, then the actual data is read in the second call
     */
    public Record read(long offset) {
        IndexEntry<Long> entry = offsetIndex.get(offset);
        return entry == null ? null : read(entry);
    }

    /**
     * Reads a single entry for the given offset, read is performed with a single IO call
     * with a buffer of size specified by readSize. If the buffer is too small for the entry, then a new one is created and
     */
    public Record read(long offset, int readSize) {
        IndexEntry<Long> entry = offsetIndex.get(offset);
        return entry == null ? null : Record.from(storage, entry.logPosition, readSize);
    }

    private Record read(IndexEntry<Long> indexEntry) {
        var hb = Buffers.allocate(RecordHeader.HEADER_BYTES, false);
        RecordHeader header = RecordHeader.readFrom(storage, hb, indexEntry.logPosition);
        return Record.readFrom(storage, header, indexEntry.logPosition);
    }

    public RecordBatchIterator batch(long startOffset, int batchSize) {
        IndexEntry<Long> ie = offsetIndex.floor(startOffset);
        if (ie == null) {
            return null;
        }
        long startPos = ie.logPosition;
        return new RecordBatchIterator(storage, startOffset, startPos, storage::position, batchSize);
    }

    public RecordIterator iterator(long startOffset) {
        IndexEntry<Long> ie = offsetIndex.floor(startOffset);
        if (ie == null) {
            return null;
        }
        long startPos = ie.logPosition;
        return new RecordIterator(storage, startOffset, startPos, storage::position);
    }

    public boolean readOnly() {
        return readOnly.get();
    }

    public boolean isFull() {
        return offsetIndex.isFull();
    }

    public void roll() {
        if (!readOnly.compareAndSet(false, true)) {
            throw new IllegalStateException("Already read only");
        }
        storage.truncate(storage.length());
    }

    public void flush() {
        storage.flush(false);
    }

    public void delete() {
        storage.delete();
        offsetIndex.delete();
    }
}
