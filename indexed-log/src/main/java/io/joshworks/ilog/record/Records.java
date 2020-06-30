package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.index.RowKey;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Queue;
import java.util.function.Consumer;

public class Records implements Iterable<Record2>, Closeable {

    final String cachePoolName;

    private final ArrayList<Record2> records = new ArrayList<>();
    private final RowKey rowKey;
    private final ByteBuffer[] buffers;
    private final Queue<Record2> cache = new ArrayDeque<>();
    private int totalSize;

    private final int maxItems;
    private final StripedBufferPool pool;

    Records(String cachePoolName, RowKey rowKey, int maxItems, StripedBufferPool pool) {
        this.rowKey = rowKey;
        this.buffers = new ByteBuffer[maxItems];
        this.cachePoolName = cachePoolName;
        this.maxItems = maxItems;
        this.pool = pool;
    }

    public int read(ByteBuffer data) {
        int read = 0;
        int i = 0;
        while (RecordBatch.hasNext(data) && i < maxItems) {
            int rsize = Record.sizeOf(data);
            ByteBuffer recdata = pool.allocate(rsize);
            Buffers.copy(data, data.position(), rsize, recdata);

            addRecord(recdata);
            read += recdata.remaining();
            i++;
        }

        return read;
    }

    private Record2 addRecord(ByteBuffer recdata) {
        Record2 record = allocateRecord();
        record.data = recdata.flip();

        if (!Record.isValid(recdata)) {
            throw new RuntimeException("Invalid record"); // should never happen
        }

        int recSize = recdata.remaining();
        buffers[records.size()] = record.data;
        records.add(record);
        totalSize += recSize;

        return record;
    }

    public Record2 add(ByteBuffer value, Consumer<ByteBuffer> keyWriter) {
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

        int recLen = Record2.HEADER_BYTES + rowKey.keySize() + valLen;

        writeHeader(recdata, value.remaining(), checksum, recLen);

        return addRecord(recdata);
    }

    private void writeHeader(ByteBuffer recdata, int valueLen, int checksum, int recLen) {
        recdata.position(0);
        recdata.putInt(recLen); // RECORD_LEN
        recdata.putInt(valueLen); // VALUE_LEN
        recdata.putInt(checksum); // CHECKSUM
        recdata.putLong(System.currentTimeMillis()); // TIMESTAMP
        recdata.put((byte) 0); // ATTRIBUTES

        recdata.limit(recLen).position(0);
    }

    //WORKAROUND FOR SEQUENCELOG THAT NEEDS TO WRITE THE KEY/VALUE FROM ONE RECORD INTO ANOTHER
    //KEY AND VALUE OF THE RECORD BECOME
    public Record2 wrap(Record2 rec, Consumer<ByteBuffer> keyWriter) {
        ByteBuffer buffer = rec.data;
        int ppos = buffer.position();
        int plim = buffer.limit();

        buffer.limit(buffer.position() + rec.valueOffset());
        buffer.position(buffer.position() + rec.valueSize());

        Record2 wrapped = add(buffer, keyWriter);
        buffer.limit(plim).position(ppos);
        return wrapped;
    }

    private Record2 allocateRecord() {
        Record2 poll = cache.poll();
        return poll == null ? new Record2(rowKey) : poll;
    }

    @Override
    public void close() {
        int i = 0;
        for (Record2 record : records) {
            pool.free(record.data);
            record.data = null;
            buffers[i] = null;
            cache.offer(record);
            RecordPool.free(this);
        }
        records.clear();
        totalSize = 0;
    }

    public int size() {
        return records.size();
    }

    public int totalSize() {
        return totalSize;
    }

    @Override
    public Iterator<Record2> iterator() {
        return records.iterator();
    }

    public long writeTo(GatheringByteChannel channel) {
        try {
            return channel.write(buffers);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to channel", e);
        }
    }

    public long writeTo(GatheringByteChannel channel, int offset, int count) {
        try {
            return channel.write(buffers, offset, count);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to channel", e);
        }
    }

    public Record2 get(int index) {
        return records.get(index);
    }
}
