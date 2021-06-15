package io.joshworks.es.writer;

import io.joshworks.es.Event;
import io.joshworks.es.StreamHasher;
import io.joshworks.es.VersionMismatch;
import io.joshworks.es.events.SystemStreams;
import io.joshworks.es.events.WriteEvent;
import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BatchingWriter {

    private final Log log;
    private final Index index;
    private final int bufferSize;
    private final int maxBatchSize;

    //staging
    private ByteBuffer writeBuffer;
    private final List<IndexEntry> stagingIndex = new ArrayList<>();

    //state
    private long segmentIdx;
    private long logPos;
    private long sequence;

    private int batchSize;

    public BatchingWriter(Log log, Index index, int maxBatchSize, int bufferSize) {
        this.log = log;
        this.index = index;
        this.maxBatchSize = maxBatchSize;
        this.writeBuffer = Buffers.allocate(bufferSize, false);

        this.segmentIdx = log.segmentIdx();
        this.logPos = log.segmentPosition();
        this.bufferSize = bufferSize;
        this.sequence = 0; //TODO fetch from store
    }

    private int validateAndGetVersion(long stream, int expectedVersion) {
        int streamVersion = version(stream);
        if (expectedVersion >= 0 && expectedVersion != streamVersion) {
            throw new VersionMismatch(expectedVersion, streamVersion);
        }
        return streamVersion + 1;
    }

    private int version(long stream) {
        for (int i = stagingIndex.size() - 1; i >= 0; i--) {
            IndexEntry ie = stagingIndex.get(i);
            if (ie.stream() == stream) {
                return ie.version();
            }
        }
        return index.version(stream);
    }

    public IndexEntry findEquals(IndexKey key) {
        for (int i = stagingIndex.size() - 1; i >= 0; i--) {
            IndexEntry ie = stagingIndex.get(i);
            if (ie.stream() == key.stream() && ie.version() == key.version()) {
                return ie;
            }
        }
        return index.get(key);
    }

    void append(WriteEvent event) {
        appendInternal(event);
        if (indexAlmostFull()) {
            appendInternal(SystemStreams.indexFlush());
            flushBatch();
            index.flush();
        }
    }

    private boolean indexAlmostFull() {
        return index.memTableSize() + stagingIndex.size() + 1 >= index.memTableCapacity();
    }

    private void appendInternal(WriteEvent event) {
        long logAddress = Log.toSegmentedPosition(segmentIdx, logPos);

        long stream = StreamHasher.hash(event.stream());
        int version = validateAndGetVersion(stream, event.expectedVersion());

        //------ STATE CHANGE -----
        int entrySize = addToBuffer(event, version, sequence);
        if (entrySize == -1) { //not added
            flushBatch();
            appendInternal(event); //try again
            return;
        }
        stagingIndex.add(new IndexEntry(stream, version, logAddress));
        this.logPos += entrySize;
        this.sequence++;
        this.batchSize++;
        //-----------------------
    }

    private int addToBuffer(WriteEvent event, int version, long sequence) {
        int entrySize = Event.sizeOf(event);
        if (entrySize > writeBuffer.remaining()) {
            writeBuffer = expandBuffer(entrySize);
        }
        int copied = Event.serialize(event, version, sequence, writeBuffer);
        assert copied == entrySize;
        return copied;
    }

    private ByteBuffer expandBuffer(int entrySize) {
        ByteBuffer newBuffer = Buffers.allocate(writeBuffer.capacity() + entrySize, false);
        writeBuffer.flip();
        Buffers.copy(writeBuffer, newBuffer);
        writeBuffer = newBuffer;
        return writeBuffer;
    }

    boolean isFull() {
        return writeBuffer.position() >= bufferSize || batchSize >= maxBatchSize;
    }

    boolean isEmpty() {
        return writeBuffer.position() == 0 && batchSize == 0;
    }

    //flush changes to disk
    void flushBatch() {
        if (batchSize == 0) {
            return;
        }
        try {
            flushLogBuffer();
            flushStagedIndex();
            batchSize = 0;
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush log", e);
        }
    }

    private void flushStagedIndex() {
        for (IndexEntry entry : stagingIndex) {
            index.append(entry);
        }
        stagingIndex.clear();
    }

    public void flushLogBuffer() {
        writeBuffer.flip();
        assert writeBuffer.hasRemaining();

        log.append(writeBuffer);

        assert !writeBuffer.hasRemaining();
        writeBuffer.compact();

        assert this.segmentIdx == log.segmentIdx() && logPos == log.segmentPosition() : "Log position mismatch";

        if (log.full()) {
            rollLog();
        }
    }

    private void rollLog() {
        log.roll();
        this.segmentIdx = log.segmentIdx();
        this.logPos = log.segmentPosition();
    }

}
