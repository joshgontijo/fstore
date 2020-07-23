package io.joshworks.es.writer;

import io.joshworks.es.Event;
import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class StoreWriter {

    private final Log log;
    private final Index index;

    private final int maxTransactions;
    private final int bufferSize; // suggestive, just used to limit writes to log
    private ByteBuffer writeBuffer; //resizable

    private final List<IndexEntry> indexEntries = new ArrayList<>();

    private final List<Transaction> transactions = new ArrayList<>();

    //state
    private int segmentIdx;
    private long logPos;
    private long sequence;
    private Transaction current;

    public StoreWriter(Log log, Index index, int maxTransactions, int bufferSize) {
        this.log = log;
        this.index = index;
        this.bufferSize = bufferSize;
        this.maxTransactions = maxTransactions;
        this.writeBuffer = Buffers.allocate(bufferSize, false);

        this.segmentIdx = log.segmentIdx();
        this.logPos = log.segmentPosition();
        this.sequence = 0; //TODO
    }

    public int version(long stream) {
        for (int i = indexEntries.size() - 1; i >= 0; i--) {
            IndexEntry entry = indexEntries.get(i);
            if (entry.stream() == stream) {
                return entry.version();
            }
        }
        return index.version(stream);
    }

    public int nextVersion(long stream, int expectedVersion) {
        int streamVersion = version(stream);
        if (expectedVersion >= 0 && expectedVersion != streamVersion) {
            throw new IllegalStateException("Version mismatch, expected " + expectedVersion + " got: " + streamVersion);
        }
        return streamVersion + 1;
    }

    public long appendToLog(WriteEvent event) {
        long logAddress = Log.toSegmentedPosition(segmentIdx, logPos);

        int entrySize = serialize(event, sequence);

        //------ STATE CHANGE -----
        this.logPos += entrySize;
        this.sequence++;
        current.add(new WriteToLog(entrySize));
        //-----------------------

        return logAddress;
    }

    private int serialize(WriteEvent event, long sequence) {
        int entrySize = Event.sizeOf(event);
        if (entrySize > writeBuffer.remaining()) {
            ByteBuffer newBuffer = Buffers.allocate(writeBuffer.capacity() + entrySize, false);
            writeBuffer.flip();
            Buffers.copy(writeBuffer, newBuffer);
            writeBuffer = newBuffer;
        }
        int copied = Event.serialize(event, sequence, writeBuffer);
        assert copied == entrySize;
        return copied;
    }

    public void adToIndex(IndexEntry entry) {
        indexEntries.add(entry);
        current.add(new WriteToIndex());
    }


    boolean isFull() {
        return writeBuffer.position() >= bufferSize || transactions.size() >= maxTransactions;
    }

    boolean isEmpty() {
        return writeBuffer.position() == 0 && transactions.isEmpty();
    }

    public void prepare() {
        assert current == null : "Active transaction";
        this.current = new Transaction();
    }

    //complete transaction unit
    public void complete() {
        assert current != null : "No active transaction";
        if (!current.units.isEmpty()) {
            transactions.add(current);
        }
        current = null;
    }

    //flush changes to disk
    void commit() {
//        System.out.println("Committing " + transactions.size() + " transactions");
        writeBuffer.flip();
        log.append(writeBuffer);
        writeBuffer.compact();
        assert writeBuffer.position() == 0;
        for (IndexEntry indexEntry : indexEntries) {
            index.append(indexEntry);
        }
        indexEntries.clear();
        transactions.clear();

        assert this.segmentIdx == log.segmentIdx() && logPos == log.segmentPosition() : "Log position mismatch";

        //rolls only after writing all transactions
        if (log.full()) {
            log.roll();
            this.segmentIdx = log.segmentIdx();
            this.logPos = log.segmentPosition();
        }
    }

    //rolls back the last attempt to write to either the index or log
    void rollback() {
        assert current != null : "No active transaction";
        for (WorkUnit unit : current.units) {
            unit.rollback();
        }
        this.current = null;
    }

    //TODO only works for EQUALS, maybe a bst here would work
    public IndexEntry findEquals(IndexKey key) {
        for (IndexEntry entry : indexEntries) {
            if (key.stream() == entry.stream() && key.version() == entry.version()) {
                return entry;
            }
        }
        return index.get(key);
    }

    private static class Transaction {
        private final List<WorkUnit> units = new ArrayList<>();

        private void add(WorkUnit unit) {
            units.add(unit);
        }
    }

    private interface WorkUnit {

        void rollback();
    }

    private class WriteToIndex implements WorkUnit {

        @Override
        public void rollback() {
            indexEntries.remove(indexEntries.size() - 1);
        }
    }

    private class WriteToLog implements WorkUnit {

        final int entrySize;

        private WriteToLog(int entrySize) {
            this.entrySize = entrySize;
        }


        @Override
        public void rollback() {
            Buffers.offsetPosition(writeBuffer, -entrySize);
            logPos -= entrySize;
            sequence--;
        }
    }

}
