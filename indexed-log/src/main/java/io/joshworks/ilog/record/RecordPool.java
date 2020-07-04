package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.index.RowKey;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.Queue;

public class RecordPool {

    //object cache
    private final Queue<BufferRecords> bufferRecordsCache = new ArrayDeque<>();
    private final Queue<ChannelRecords> channelRecordsCache = new ArrayDeque<>();
    private final Queue<SegmentRecords> segmentRecordsCache = new ArrayDeque<>();

    private final StripedBufferPool pool;
    private final RowKey rowKey;
    private final int readBufferSize;
    private final int batchSize;

    RecordPool(StripedBufferPool pool, RowKey rowKey, int readBufferSize, int batchSize) {
        this.pool = pool;
        this.rowKey = rowKey;
        this.readBufferSize = readBufferSize;
        this.batchSize = batchSize;
    }

    public static PoolConfig create(RowKey rowKey) {
        return new PoolConfig(rowKey);
    }

    public RowKey rowKey() {
        return rowKey;
    }

    public BufferRecords fromBuffer(ByteBuffer data) {
        BufferRecords records = getBufferRecords();

        int i = 0;
        int copied = 0;
        while (RecordBatch.hasNext(data) && i < batchSize) {
            int rsize = Record.sizeOf(data);
            ByteBuffer recData = pool.allocate(rsize);
            copied += Buffers.copy(data, data.position(), rsize, recData);

            records.add(recData);
            i++;
        }
        Buffers.offsetPosition(data, copied);
        return records;
    }

    public AbstractChannelRecords fromChannel(ReadableByteChannel channel) {
        ChannelRecords records = getChannelRecords();
        records.init(readBufferSize, channel);
        return records;
    }

    public SegmentRecords fromSegment(IndexedSegment segment) {
        return fromSegment(segment, IndexedSegment.START);
    }

    public SegmentRecords fromSegment(IndexedSegment segment, long startPos) {
        SegmentRecords records = getSegmentRecords();
        records.init(readBufferSize, segment, startPos);
        return records;
    }

    private BufferRecords getBufferRecords() {
        BufferRecords records = bufferRecordsCache.poll();
        if (records == null) {
            return new BufferRecords(this, rowKey, batchSize);
        }
        return records;
    }

    private ChannelRecords getChannelRecords() {
        ChannelRecords records = channelRecordsCache.poll();
        if (records == null) {
            return new ChannelRecords(this);
        }
        return records;
    }

    private SegmentRecords getSegmentRecords() {
        SegmentRecords records = segmentRecordsCache.poll();
        if (records == null) {
            return new SegmentRecords(this);
        }
        return records;
    }

    ByteBuffer allocate(int size) {
        return pool.allocate(size);
    }

    void free(ByteBuffer buffer) {
        pool.free(buffer);
    }

    void free(Records records) {
        if (records == null) {
            return;
        }
        if (records instanceof BufferRecords) {
            bufferRecordsCache.offer((BufferRecords) records);
        } else if (records instanceof ChannelRecords) {
            channelRecordsCache.offer((ChannelRecords) records);
        } else if (records instanceof SegmentRecords) {
            segmentRecordsCache.offer((SegmentRecords) records);
        } else {
            throw new RuntimeException("Unknown type: " + records.getClass());
        }
    }


}
