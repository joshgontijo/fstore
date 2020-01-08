package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.FileUtils;
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
import java.util.function.BiFunction;

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class IndexedSegment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    public static long START = 0;

    private final File file;
    private final FileChannel channel;
    private Index index;

    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final AtomicLong writePosition = new AtomicLong();

    public IndexedSegment(File file, Index index) {
        this.file = file;
        this.index = index;
        boolean newSegment = FileUtils.createIfNotExists(file);
        this.readOnly.set(!newSegment);
        this.channel = openChannel(file);
        if (!newSegment) {
            seekEndOfLog();
        }
    }

    public void reindex(BiFunction<File, Integer, Index> indexFactory) throws IOException {
        log.info("Reindexing {}", name());

        int indexSize = index.size();
        index.delete();
        File indexFile = View.indexFile(file);
        this.index = indexFactory.apply(indexFile, indexSize);

        long start = System.currentTimeMillis();
        RecordBatchIterator it = new RecordBatchIterator(channel, START, writePosition, 4096);
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
        int recordKeyLen = record.keyLength();
        if (recordKeyLen != keyLen) { // validates the key BEFORE adding to log
            throw new IllegalArgumentException("Invalid key length: Expected " + keyLen + ", got " + recordKeyLen);
        }

        try {
            int written = record.appendTo(channel);
            long position = writePosition.getAndAdd(written);
            index.write(record, position);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to append record", e);
        }
    }


    /**
     * Reads a single entry for the given offset, read is performed with a two IO calls
     * first one to read the header, then the actual data is read in the second call
     */
    public Record read(ByteBuffer key) throws IOException {
        long position = index.get(key);
        if (position < START) {
            return null;
        }

        var headerBuffer = Buffers.allocate(HEADER_BYTES, false);
        channel.read(headerBuffer, position);
        headerBuffer.flip();
        if (headerBuffer.remaining() < HEADER_BYTES) {
            return null;
        }
        int recordLen = Record.recordLength(headerBuffer);
        var recordBuffer = Buffers.allocate(recordLen, false);
        channel.read(recordBuffer, position);
        recordBuffer.flip();
        return Record.from(recordBuffer, false);

    }

    /**
     * Reads a single entry for the given offset, read is performed with a single IO call
     * with a buffer of size specified by readSize. If the buffer is too small for the entry, then a new one is created and
     */
    public Record read(ByteBuffer key, int readSize) throws IOException {
        long position = index.get(key);
        if (position < START) {
            return null;
        }

        if (readSize <= HEADER_BYTES) {
            throw new RuntimeException("bufferSize must be greater than " + HEADER_BYTES);
        }
        ByteBuffer buffer = Buffers.allocate(readSize, false);
        channel.read(buffer, position);
        buffer.flip();
        if (buffer.remaining() < HEADER_BYTES) {
            return null;
        }
        int recordLen = Record.recordLength(buffer);
        if (recordLen > buffer.remaining()) {
            //too big, re read with a bigger buffer
            return read(key, recordLen);
        }
        return Record.from(buffer, false);
    }

    public RecordBatchIterator iterator(ByteBuffer start, int batchSize) {
        long startPos = index.floor(start);
        if (startPos < START) {
            return null; //TODO return empty iterator
        }
        return new RecordBatchIterator(channel, startPos, writePosition, batchSize);
    }

    public RecordBatchIterator iterator(int batchSize) {
        return new RecordBatchIterator(channel, START, writePosition, batchSize);
    }

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
        return View.segmentIdx(name());
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
