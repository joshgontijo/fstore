package io.joshworks.ilog;

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

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class IndexedSegment<K extends Comparable<K>> {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    private final File file;
    private final int indexSize;
    private final KeyParser<K> parser;
    private final FileChannel channel;
    private Index<K> index;

    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final AtomicLong writePosition = new AtomicLong();

    public IndexedSegment(File file, int indexSize, KeyParser<K> parser) throws IOException {
        this.file = file;
        this.indexSize = indexSize;
        this.parser = parser;
        boolean newFile = FileUtils.createIfNotExists(file);
        this.readOnly.set(!newFile);
        this.channel = open(file, readOnly.get());
        this.index = openIndex(file);
    }

    private Index<K> openIndex(File segmentFile) {
        String name = segmentFile.getName().split("\\.")[0];
        File dir = segmentFile.toPath().getParent().toFile();
        File idxFile = new File(dir, name + ".index");
        return new Index<>(idxFile, indexSize, parser);
    }

    public void reindex() throws IOException {
        log.info("Restoring segment {}", file.getAbsolutePath());

        index.delete();
        index = openIndex(file);

        long start = System.currentTimeMillis();
        RecordBatchIterator it = new RecordBatchIterator(channel, 0, writePosition, 4096);
        while (it.hasNext()) {
            long position = it.position();
            Record record = it.next();
            index.write(record, position);
        }
        log.info("Restoring of {} completed in {}ms", file.getAbsolutePath(), System.currentTimeMillis() - start);
    }

    private FileChannel open(File file, boolean readOnly) throws IOException {
        if (readOnly) {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ);
        }
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.force(true);
        return channel;
    }

    synchronized void append(Record record) throws IOException {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }

        int keyLen = index.keySize();
        int recordKeyLen = record.keyLength();
        if (recordKeyLen != keyLen) {
            throw new IllegalArgumentException("Invalid key length: Expected " + keyLen + ", got " + recordKeyLen);
        }

        int written = record.appendTo(channel);
        long position = writePosition.getAndAdd(written);
        index.write(record, position);
    }


    /**
     * Reads a single entry for the given offset, read is performed with a two IO calls
     * first one to read the header, then the actual data is read in the second call
     */
    public Record read(K key) throws IOException {
        long position = index.get(key);
        if (position < 0) {
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
    public Record read(K key, int readSize) throws IOException {
        long position = index.get(key);
        if (position < 0) {
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

    public RecordBatchIterator batch(K start, int batchSize) {
        long startPos = index.floor(start);
        if (startPos < 0) {
            return null; //TODO return empty iterator
        }
        return new RecordBatchIterator(channel, startPos, writePosition, batchSize);
    }

    public boolean readOnly() {
        return readOnly.get();
    }

    public boolean isFull() {
        return index.isFull();
    }

    public long entries() {
        return index.entries();
    }

    public void roll() throws IOException {
        if (!readOnly.compareAndSet(false, true)) {
            throw new IllegalStateException("Already read only");
        }
        long fileSize = size();
        channel.truncate(fileSize);
        writePosition.set(fileSize);
    }

    public long size() throws IOException {
        return channel.size();
    }

    public void flush(boolean metadata) throws IOException {
        channel.force(metadata);
    }

    public void delete() throws IOException {
        channel.close();
        Files.delete(file.toPath());
        index.delete();
    }
}
