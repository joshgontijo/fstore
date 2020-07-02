package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.joshworks.ilog.index.Index.NONE;

public class IndexedSegment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    public static long START = 0;

    private final File file;
    private final int indexSize;
    protected final RowKey comparator;
    private final FileChannel channel;
    private final long id;
    protected Index index;

    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final AtomicLong writePosition = new AtomicLong();
    private final AtomicBoolean markedForDeletion = new AtomicBoolean();
    private final Set<SegmentIterator> iterators = new HashSet<>();

    public IndexedSegment(File file, int indexSize, RowKey comparator) {
        this.file = file;
        this.indexSize = indexSize;
        this.comparator = comparator;
        this.index = openIndex(file, indexSize, comparator);
        this.id = LogUtil.segmentId(file.getName());
        boolean newSegment = FileUtils.createIfNotExists(file);
        this.readOnly.set(!newSegment);
        this.channel = openChannel(file);
        if (!newSegment) {
            seekEndOfLog();
        }
    }

    private Index openIndex(File file, int indexSize, RowKey comparator) {
        File indexFile = LogUtil.indexFile(file);
        return new Index(indexFile, indexSize, comparator);
    }

    void reindex() throws IOException {
        log.info("Reindexing {}", name());

        index.delete();
        File indexFile = LogUtil.indexFile(file);
        FileUtils.deleteIfExists(indexFile);
        this.index = openIndex(indexFile, indexSize, comparator);

        long start = System.currentTimeMillis();
        SegmentIterator it = iterator(START, pool);
        int processed = 0;

        while (it.hasNext()) {
            long position = it.position();
            ByteBuffer record = it.next();
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
            long size = channel.size();
            writePosition.set(size);
            channel.position(size);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to set position at the of the log");
        }
    }

    //return number of written items
    public int append(Records records, int offset) {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        if (records.size() == 0) {
            return 0;
        }

        long logStartPos = writePosition();

        int remainingEntries = index.remaining();
        int maxItems = Math.min(remainingEntries, records.size());
        long written = records.writeTo(channel, offset, maxItems);
        writePosition.getAndAdd(written);
        index.write(records, offset, maxItems, logStartPos);
        return maxItems;
    }

    public int find(ByteBuffer key, ByteBuffer dst, IndexFunctions func) {
        int idx = index.find(key, func);
        if (idx == NONE) {
            return 0;
        }

        int plim = dst.limit();
        int ppos = dst.position();
        try {
            long pos = index.readPosition(idx);
            int len = index.readEntrySize(idx);

            if (len > dst.remaining()) {
                throw new IllegalStateException("Destination buffer remaining bytes is less than entry size");
            }

            Buffers.offsetLimit(dst, len);

            int read = read(pos, dst);
            if (read != len) {
                throw new IllegalStateException("Expected read of " + len + " actual read: " + read);
            }
            dst.limit(plim);

            return read;
        } catch (Exception e) {
            dst.limit(plim).position(ppos);
            throw new RuntimeIOException(e);
        }
    }

    /**
     * Read data starting from the position of the given key
     * Data fill dst with available bytes
     */
    public int bulkRead(ByteBuffer key, ByteBuffer dst, IndexFunctions func) {
        int idx = index.find(key, func);
        if (idx == NONE) {
            return 0;
        }
        try {
            long pos = index.readPosition(idx);
            return read(pos, dst);
        } catch (Exception e) {
            throw new RuntimeIOException(e);
        }
    }

    /**
     * Reads a single entry for the given offset, read is performed with a single IO call
     * with a buffer of size specified by readSize. If the buffer is too small for the entry, then a new one is created and
     */
    public int read(long position, ByteBuffer dst) {
        try {
            long writePos = writePosition();
            if (position >= writePos) {
                return readOnly() ? -1 : 0;
            }

            int count = (int) Math.min(dst.remaining(), writePos - position);
            assert count > 0;
            int plim = dst.limit();
            Buffers.offsetLimit(dst, count);
            int read = channel.read(dst, position);
            dst.limit(plim);
            assert read == count;
            return read;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public boolean readOnly() {
        return readOnly.get();
    }

    public boolean isFull() {
        return index.isFull();
    }

    public int remaining() {
        return index.remaining();
    }

    public int indexSize() {
        return index.size();
    }

    public long entries() {
        return index.entries();
    }

    public void forceRoll() throws IOException {
        flush();
        long fileSize = writePosition();
        writePosition.set(fileSize);
        channel.truncate(fileSize);
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

    public void flush() {
        try {
            channel.force(false);
            index.flush();
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to flush segment", e);
        }
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
        return LogUtil.levelOf(id);
    }

    public long segmentId() {
        return id;
    }

    public long segmentIdx() {
        return LogUtil.segmentIdx(id);
    }

    public long writePosition() {
        return writePosition.get();
    }

    public synchronized void delete() {
        if (!markedForDeletion.compareAndSet(false, true)) {
            return;
        }
        if (!iterators.isEmpty()) {
            log.info("Segment marked for deletion");
            return;
        }
        doDelete();
    }

    public synchronized SegmentIterator iterator(long pos, BufferPool pool) {
        SegmentIterator it = new SegmentIterator(this, pos, pool);
        iterators.add(it);
        return it;
    }

    private void doDelete() {
        if (!iterators.isEmpty()) {
            throw new IllegalStateException("Segment in being used");
        }
        try {
            log.info("Deleting {}", name());
            channel.close();
            Files.delete(file.toPath());
            index.delete();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    void release(SegmentIterator iterator) {
        iterators.remove(iterator);
        if (markedForDeletion.get()) {
            synchronized (this) {
                doDelete();
            }
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
