package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.Queue;

import static io.joshworks.ilog.index.Index.NONE;

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

    public BufferRecords empty() {
        return getBufferRecords();
    }

    public BufferRecords fromBuffer(ByteBuffer data) {
        BufferRecords records = getBufferRecords();
        if (!data.hasRemaining()) {
            return records;
        }

        int i = 0;
        while (RecordBatch.hasNext(data) && i < batchSize) {
            int rsize = RecordUtils.sizeOf(data);
            ByteBuffer recData = pool.allocate(rsize);
            Buffers.copy(data, data.position(), rsize, recData);
            RecordBatch.advance(data);

            recData.flip();
            records.add(recData);

            i++;
        }

        return records;
    }

    public Records fromChannel(ReadableByteChannel channel) {
        ChannelRecords records = getChannelRecords();
        records.init(readBufferSize, channel);
        return records;
    }

    public Records fromSegment(IndexedSegment segment) {
        return fromSegment(segment, IndexedSegment.START);
    }

    public Records fromSegment(IndexedSegment segment, long startPos) {
        SegmentRecords records = getSegmentRecords();
        records.init(readBufferSize, segment, startPos);
        return records;
    }

    public Records read(IndexedSegment segment, ByteBuffer key, IndexFunction func) {
        Index index = segment.index();
        int idx = index.find(key, func);
        if (idx == NONE) {
            return Records.EMPTY;
        }
        long pos = index.readPosition(idx);
        int len = index.readEntrySize(idx);

        ByteBuffer buffer = allocate(len);
        try {
            segment.channel().read(buffer, pos);
            buffer.flip();
            return fromBuffer(buffer);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read entry", e);
        } finally {
            free(buffer);
        }
    }

    BufferRecords getBufferRecords() {
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
