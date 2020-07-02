package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.RowKey;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayDeque;
import java.util.Queue;

public class BufferRecords extends AbstractRecords {

    //the active records
    private final Queue<Record2> records = new ArrayDeque<>();

    //the active record buffers
    private final Queue<ByteBuffer> buffers = new ArrayDeque<>();

    //the object cache where all instances should return to
    private final Queue<Record2> cache = new ArrayDeque<>();

    //The buffer poll where all data should return to
    private final StripedBufferPool pool;

    //temp holder for buffers so it can gather write to a channel
    private final ByteBuffer[] tmp;

    protected final RowKey rowKey;
    private int totalSize;

    BufferRecords(String poolName, RowKey rowKey, int maxItems, StripedBufferPool pool) {
        super(poolName);
        this.tmp = new ByteBuffer[maxItems];
        this.rowKey = rowKey;
        this.pool = pool;
    }

    //copied the record into this pool
    int add(Record2 record) {
        ByteBuffer recData = pool.allocate(record.recordSize());
        int copied = Buffers.copy(record.data, recData);
        if (copied > 0) {
            add(recData);
        }
        return copied;
    }

    void add(ByteBuffer data) {
        Record2 record = allocateRecord();

        data.flip();
        if (!Record.isValid(data)) {
            pool.free(data);
            throw new RuntimeException("Invalid record"); // should never happen
        }

        record.data = data;
        int recSize = data.remaining();

        records.add(record);
        buffers.add(data);
        totalSize += recSize;
    }

    //TODO MOVE TO SOMEWHERE ELSE
//    public void create(long sequence, ByteBuffer value, Consumer<ByteBuffer> keyWriter) {
//        int rsize = Record2.HEADER_BYTES + Integer.BYTES + value.remaining();
//        ByteBuffer recdata = pool.allocate(rsize);
//        recdata.position(Record2.KEY_OFFSET);
//        keyWriter.accept(recdata);
//        if (recdata.position() - Record2.KEY_OFFSET != rowKey.keySize()) {
//            throw new IllegalStateException("Invalid row key size");
//        }
//        int checksum = ByteBufferChecksum.crc32(value);
//        int valLen = value.remaining();
//        Buffers.copy(value, recdata);
//
//        int recLen = Record2.HEADER_BYTES + rowKey.keySize() + valLen;
//
//        writeHeader(recdata, sequence, value.remaining(), checksum, recLen);
//
//        add(recdata);
//    }
//
//    private void writeHeader(ByteBuffer recdata, long sequence, int valueLen, int checksum, int recLen) {
//        recdata.position(0);
//        recdata.putInt(recLen); // RECORD_LEN
//        recdata.putInt(valueLen); // VALUE_LEN
//        recdata.putLong(sequence); // SEQUENCE
//        recdata.putInt(checksum); // CHECKSUM
//        recdata.putLong(System.currentTimeMillis()); // TIMESTAMP
//        recdata.put((byte) 0); // ATTRIBUTES
//
//        recdata.limit(recLen).position(0);
//    }

    @Override
    public Record2 poll() {
        Record2 poll = records.poll();
        if (poll == null) {
            return null;
        }
        buffers.remove(poll.data);
        totalSize -= poll.recordSize();
        return poll;
    }

    @Override
    public Record2 peek() {
        return records.peek();
    }

    private Record2 allocateRecord() {
        Record2 poll = cache.poll();
        return poll == null ? new Record2(this, rowKey) : poll;
    }

    @Override
    public void close() {
        for (Record2 record : records) {
            release(record);
            RecordPool.free(this);
        }
        records.clear();
        totalSize = 0;
    }

    void release(Record2 record) {
        if (record.owner != this) {
            throw new IllegalArgumentException("Record pool " + this.poolName + "not owner of this record: pool name " + record.owner.poolName);
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

    public int totalSize() {
        return totalSize;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        return writeTo(channel, buffers.size());
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count) {
        try {
            buffers.toArray(tmp);

            count = Math.min(count, buffers.size());
            return channel.write(tmp, 0, count);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to channel", e);
        }
    }

}
