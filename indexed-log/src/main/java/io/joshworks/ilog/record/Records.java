package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.ilog.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class Records extends AbstractRecords implements Iterable<Record2> {

    //the active records
    private final List<Record2> records = new ArrayList<>();

    //the active record buffers
    private final ByteBuffer[] buffers;

    //the object cache where all instances should return to
    private final Queue<Record2> cache = new ArrayDeque<>();

    //single, reusable, non thread-safe iterator
    private final RecordIterator iterator = new RecordIterator(new ArrayIt());
    private int itIdx;

    Records(RecordPool pool, int maxItems) {
        super(pool);
        this.buffers = new ByteBuffer[maxItems];
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

    public int add(ByteBuffer data) {
        if (!data.hasRemaining()) {
            return 0;
        }

        int i = 0;
        while (RecordBatch.hasNext(data) && !isFull()) {
            int rsize = RecordUtils.sizeOf(data);
            ByteBuffer recData = pool.allocate(rsize);
            Buffers.copy(data, data.position(), rsize, recData);
            RecordBatch.advance(data);

            recData.flip();
            if (!RecordUtils.isValid(recData)) {
                throw new RuntimeException("Invalid record");
            }
            Record2 record = allocateEmptyRecord();
            record.parse(recData);
            add(record);
            i++;
        }

        return i;
    }

    public void add(ByteBuffer key, ByteBuffer value, int... attr) {
        add(key, key.position(), key.remaining(), value, value.position(), value.remaining(), attr);
    }

    public void add(ByteBuffer key, int kOffset, int kLen, ByteBuffer value, int vOffset, int vLen, int... attr) {
        int totalSize = Record2.getRecordSize(kLen, vLen);
        ByteBuffer dst = pool.allocate(totalSize);

        Record2 rec = allocateEmptyRecord();
        rec.create(dst, key, kOffset, kLen, value, vOffset, vLen, attr);

        rec.parse(dst);
    }

    public int from(FileChannel channel, long position, int count) {
        try {
            ByteBuffer dst = pool.allocate(count);
            dst.limit(count);
            int read = channel.read(dst, position);
            dst.flip();

            this.add(dst);

            return read;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read entry: " + channel + ", position: " + position + ", count: " + count, e);
        }
    }

    private Record2 allocateEmptyRecord() {
        Record2 poll = cache.poll();
        return poll == null ? new Record2() : poll;
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

    public void clear() {
        itIdx = 0;
        for (int i = 0; i < size(); i++) {
            ByteBuffer recBuffer = records.get(i).free();
            assert recBuffer == buffers[i];
            pool.free(buffers[i]);
            buffers[i] = null;
        }
        cache.addAll(records);
        records.clear();
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

    public long writeTo(GatheringByteChannel channel) {
        return writeTo(channel, 0, size());
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
