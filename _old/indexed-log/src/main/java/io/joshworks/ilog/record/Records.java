package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Channels;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Iterators;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class Records extends AbstractRecords implements Iterable<Record> {

    //the active records
    private final List<Record> records = new ArrayList<>();

    //the active record buffers
    private final ByteBuffer[] buffers;

    //single, reusable, non thread-safe iterator
    private final RecordIterator iterator = new RecordIterator();
    private int itIdx;

    Records(RecordPool pool, int maxItems) {
        super(pool);
        this.buffers = new ByteBuffer[maxItems];
    }

    //copied the record into this pool
    public boolean add(Record record) {
        if (size() >= buffers.length) {
            return false;
        }
        ByteBuffer recData = pool.allocate(record.recordSize());
        int copied = record.copyTo(recData);
        assert copied == record.recordSize();
        recData.flip();
        addInternal(recData);
        return true;
    }

    public int add(ByteBuffer data, int offset, int count) {
        if (count == 0) {
            return 0;
        }

        int bytes = 0;
        while (Record.isValid(data, offset) && records.size() < buffers.length) {
            int recSize = Record.recordSize(data, offset);
            if (bytes + recSize > count) { //count reached stop parsing
                break;
            }
            ByteBuffer dst = pool.allocate(recSize);
            int copied = Buffers.copy(data, offset, recSize, dst);
            assert copied == recSize;
            offset += copied;
            bytes += copied;
            dst.flip();
            addInternal(dst);
        }
        return bytes;
    }

    public int add(ByteBuffer data) {
        int bytes = add(data, data.position(), data.remaining());
        Buffers.offsetPosition(data, bytes);
        return bytes;
    }

    private void addInternal(ByteBuffer recData) {
        Record rec = pool.allocateRecord();
        rec.init(recData);
        buffers[records.size()] = recData;
        records.add(rec);
    }

    public void add(ByteBuffer key, ByteBuffer value, int... attr) {
        add(key, key.position(), key.remaining(), value, value.position(), value.remaining(), attr);
    }

    public void add(ByteBuffer key, int kOffset, int kLen, ByteBuffer value, int vOffset, int vLen, int... attr) {
        int totalSize = Record.computeRecordSize(kLen, vLen);
        ByteBuffer dst = pool.allocate(totalSize);

        Record rec = pool.allocateRecord();
        rec.create(dst, key, kOffset, kLen, value, vOffset, vLen, attr);

        buffers[records.size()] = dst;
        records.add(rec);
    }

    @Override
    public void close() {
        clear();
        pool.free(this);
    }


    public int size() {
        return records.size();
    }

    public boolean isFull() {
        return records.size() >= buffers.length;
    }

    public boolean isEmpty() {
        return records.size() == 0;
    }

    public void clear() {
        itIdx = 0;
        for (int i = 0; i < size(); i++) {
            Record record = records.get(i);
            record.close();
            buffers[i] = null;
        }
        records.clear();
    }

    public Record get(int idx) {
        return records.get(idx);
    }

    public long writeTo(GatheringByteChannel channel, int offset, int count) {
        if (offset + count > size()) {
            throw new IndexOutOfBoundsException();
        }
        try {
            return Channels.writeFully(channel, buffers, offset, count);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to transfer data", e);
        }
    }

    public long writeTo(GatheringByteChannel channel) {
        return writeTo(channel, 0, size());
    }

    @Override
    public RecordIterator iterator() {
        itIdx = 0;
        return iterator;
    }

    public class RecordIterator implements Iterators.CloseableIterator<Record> {

        @Override
        public boolean hasNext() {
            return itIdx < records.size();
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record record = records.get(itIdx);
            if (record != null) {
                itIdx++;
            }
            return record;
        }

        public Record peek() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return records.get(itIdx);
        }

        @Override
        public void close() {
            //do nothing
        }
    }

}
