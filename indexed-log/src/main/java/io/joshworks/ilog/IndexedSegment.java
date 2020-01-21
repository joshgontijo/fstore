package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.KeyComparator;
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

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class IndexedSegment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    public static long START = 0;

    private final File file;
    private final int indexSize;
    private final KeyComparator comparator;
    private final FileChannel channel;
    private final long id;
    private Index index;

    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final AtomicLong writePosition = new AtomicLong();

    public IndexedSegment(File file, int indexSize, KeyComparator comparator) {
        this.file = file;
        this.indexSize = indexSize;
        this.comparator = comparator;
        this.index = openIndex(file, indexSize, comparator);
        this.id = View.segmentIdx(file.getName());
        boolean newSegment = FileUtils.createIfNotExists(file);
        this.readOnly.set(!newSegment);
        this.channel = openChannel(file);
        if (!newSegment) {
            seekEndOfLog();
        }
    }

    private Index openIndex(File file, int indexSize, KeyComparator comparator) {
        File indexFile = View.indexFile(file);
        return new Index(indexFile, indexSize, comparator);
    }

    void reindex(BufferPool pool) throws IOException {
        log.info("Reindexing {}", name());

        index.delete();
        File indexFile = View.indexFile(file);
        FileUtils.deleteIfExists(indexFile);
        this.index = openIndex(indexFile, indexSize, comparator);

        long start = System.currentTimeMillis();
        RecordBatchIterator it = new RecordBatchIterator(this, START, pool);
        int processed = 0;
        while (it.hasNext()) {
            long position = it.position();
            Record record = it.next();
            index.write(record, position);
            processed++;
        }
        log.info("Restored {}: {} entries in {}ms", name(), processed, System.currentTimeMillis() - start);
    }

    private FileChannel openChannel(File file) {
        try {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to open segment", e);
        }
    }

    private void seekEndOfLog() {
        try {
            writePosition.set(channel.size());
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to set position at the of the log");
        }
    }

    public void append(Record record) {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }

        int keyLen = index.keySize();
        int recordKeyLen = record.keySize();
        if (recordKeyLen != keyLen) { // validates the key BEFORE adding to log
            throw new IllegalArgumentException("Invalid key length: Expected " + keyLen + ", got " + recordKeyLen);
        }

        try {
            int written = record.writeTo(channel);
            if (written <= 0) {
                throw new RuntimeIOException("Failed to write entry");
            }
            long position = writePosition.getAndAdd(written);
            index.write(record, position);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to append record", e);
        }
    }

    /**
     * Lookup for the entry position based on a key
     */
    public long find(ByteBuffer key) {
        return index.get(key);
    }

    /**
     * Reads a single entry for the given offset, read is performed with a single IO call
     * with a buffer of size specified by readSize. If the buffer is too small for the entry, then a new one is created and
     */
    public int read(long position, ByteBuffer dst) throws IOException {
        int dstRemaining = dst.remaining();
        if (dstRemaining <= HEADER_BYTES) {
            throw new RuntimeException("bufferSize must be greater than " + HEADER_BYTES);
        }
        return channel.read(dst, position);
    }

//    public RecordBatchIterator iterator(ByteBuffer start, int batchSize) {
//        long startPos = index.floor(start);
//        if (startPos < START) {
//            return null; //TODO return empty iterator
//        }
//        return new RecordBatchIterator(channel, startPos, writePosition, batchSize);
//    }
//
//    public RecordBatchIterator iterator() {
//        return iterator(Memory.PAGE_SIZE);
//    }
//
//    public RecordBatchIterator iterator(int batchSize) {
//        return new RecordBatchIterator(channel, START, writePosition, batchSize);
//    }

    public boolean readOnly() {
        return readOnly.get();
    }

    public boolean isFull() {
        return index.isFull();
    }

    public int indexSize() {
        return index.size();
    }

    public long entries() {
        return index.entries();
    }

    public void forceRoll() throws IOException {
        flush();
        long fileSize = size();
        channel.truncate(fileSize);
        writePosition.set(fileSize);
        index.complete();
    }

    public void roll() throws IOException {
        if (!readOnly.compareAndSet(false, true)) {
            throw new IllegalStateException("Already read only: " + name());
        }
        forceRoll();
    }

    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public void flush() throws IOException {
        channel.force(false);
    }

    public String name() {
        return file.getName();
    }

    public File file() {
        return file;
    }

    public String index() {
        return index.name();
    }

    public int level() {
        return View.levelOf(name());
    }

    public long segmentId() {
        return id;
    }

    public void delete() {
        try {
            channel.close();
            Files.delete(file.toPath());
            index.delete();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public void close() throws IOException {
        channel.close();
        index.close();
    }

    @Override
    public String toString() {
        return "IndexedSegment{" +
                "name=" + file.getName() +
                ", writePosition=" + writePosition +
                ", entries=" + entries() +
                ", indexSize=" + indexSize() +
                '}';
    }

}
