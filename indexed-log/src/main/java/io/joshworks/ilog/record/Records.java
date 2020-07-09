package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.RowKey;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Consumer;

public class Records extends AbstractRecords implements Iterable<Record2> {

    //the active records
    private final List<Record2> records = new ArrayList<>();

    //the active record buffers
    private final ByteBuffer[] buffers;

    //the object cache where all instances should return to
    private final Queue<Record2> cache = new ArrayDeque<>();

    protected final RowKey rowKey;

    //single, reusable, non thread-safe iterator
    private final RecordIterator iterator = new RecordIterator(new ArrayIt());
    private int itIdx;

    Records(RecordPool pool, RowKey rowKey, int maxItems) {
        super(pool);
        this.buffers = new ByteBuffer[maxItems];
        this.rowKey = rowKey;
    }

    //copied the record into this pool
    public boolean add(Record2 record) {
        if (size() >= buffers.length) {
            return false;
        }
        ByteBuffer recData = pool.allocate(record.recordSize());
        int copied = record.copyTo(recData);
        if (copied > 0) {
            recData.flip();
            add(recData);
        }
        return true;
    }

    void add(ByteBuffer data) {
        if (!RecordUtils.isValid(data)) {
            pool.free(data);
            throw new RuntimeException("Invalid record"); // should never happen
        }

        Record2 record = allocateEmptyRecord();
        record.data = data;

        buffers[records.size()] = data;
        records.add(record);
    }

    //TODO MOVE TO SOMEWHERE ELSE
    public void create(long sequence, ByteBuffer value, Consumer<ByteBuffer> keyWriter) {
        int rsize = Record2.HEADER_BYTES + Integer.BYTES + value.remaining();
        ByteBuffer recdata = pool.allocate(rsize);
        recdata.position(Record2.KEY_OFFSET);
        keyWriter.accept(recdata);
        if (recdata.position() - Record2.KEY_OFFSET != rowKey.keySize()) {
            throw new IllegalStateException("Invalid row key size");
        }
        int checksum = ByteBufferChecksum.crc32(value);
        int valLen = value.remaining();
        Buffers.copy(value, recdata);

        recdata.position(0);
        writeHeader(recdata, sequence, rowKey.keySize(), valLen, checksum);

        add(recdata);
    }

    private static void writeHeader(ByteBuffer dst, long sequence, int keyLen, int valueLen, int checksum) {
        int recLen = Record2.HEADER_BYTES + keyLen + valueLen;

        int ppos = dst.position();

        dst.putInt(recLen); // RECORD_LEN
        dst.putInt(valueLen); // VALUE_LEN
        dst.putLong(sequence); // SEQUENCE
        dst.putInt(checksum); // CHECKSUM
        dst.putLong(System.currentTimeMillis()); // TIMESTAMP
        dst.put((byte) 0); // ATTRIBUTES

        dst.limit(ppos + recLen).position(ppos);
    }

    private Record2 allocateEmptyRecord() {
        Record2 poll = cache.poll();
        return poll == null ? new Record2(this, rowKey) : poll;
    }

    @Override
    public void close() {
        clear();
        pool.free(this);
        records.clear();
    }

    void free(Record2 record) {
        if (record.owner != this) {
            throw new IllegalArgumentException("Record pool not owner of this record");
        }

        Record2 first = records.peek();
        //record was peeked and closed, remove from queue
        if (first != null && first.equals(record)) {
            records.poll();
        }

        pool.free(record.data);
        record.data = null;
        cache.add(record);
    }

    public int size() {
        return records.size();
    }

    public boolean isFull() {
        return records.size() >= buffers.length;
    }

    public void clear() {
        itIdx = 0;
        for (int i = 0; i < size(); i++) {
            pool.free(buffers[i]);
            buffers[i] = null;
            records.get(i).data = null;
        }
        cache.addAll(records);
    }

    public Record2 get(int idx) {
        return records.get(idx);
    }

    public long writeTo(GatheringByteChannel channel, int offset, int count) {
        if (offset + count >= size()) {
            throw new IndexOutOfBoundsException();
        }
        try {
            return Buffers.writeFully(channel, buffers, offset, count);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to transfer data", e);
        }
    }

    @Override
    public long writeTo(IndexedSegment segment) {

    }

    private void checkClosed(long w) {
        if (w == -1) {
            throw new RuntimeIOException("Channel closed");
        }
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        return writeTo(channel, buffers.size());
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count) {
        try {
            long totalWritten = 0;
            while (hasNext()) {
                buffers.toArray(tmp);
                count = Math.min(count, buffers.size());
                long written = Buffers.writeFully(channel, tmp, 0, count);
                checkClosed(written);
                totalWritten += written;

                removeWrittenEntries(written);
            }
            return totalWritten;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to channel", e);
        }
    }

    private void removeWrittenEntries(long written) {
        //removes only fully written entries, leftovers will be retried next iteration
        int bytesRemoved = 0;
        while (bytesRemoved < written) {
            Record2 rec = peek();
            int recordSize = rec.recordSize();
            if (bytesRemoved + recordSize <= written) {
                remove();
                bytesRemoved += recordSize;
            }
        }
    }

    public Records copy() {
        Records copy = pool.getBufferRecords();
        for (Record2 record : records) {
            copy.add(record);
        }
        return copy;
    }

    @Override
    public RecordIterator iterator() {
        itIdx = 0;
        return iterator;
    }

    private class ArrayIt implements Iterators.CloseableIterator<Record2> {

        @Override
        public boolean hasNext() {
            return itIdx < records.size();
        }

        @Override
        public Record2 next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return records.get(itIdx++);
        }

        @Override
        public void close() {
            //do nothing
        }
    }

}
