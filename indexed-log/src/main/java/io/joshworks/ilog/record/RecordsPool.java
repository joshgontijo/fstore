package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class RecordsPool {

    //object cache
    private static final Map<String, Queue<BufferRecords>> bufferRecordsCache = new HashMap<>();
    private static final Map<String, Queue<ChannelRecords>> channelRecordsCache = new HashMap<>();
    private static final Map<String, Queue<SegmentRecords>> segmentRecordsCache = new HashMap<>();

    private static final Map<String, PoolConfig> poolConfig = new HashMap<>();
    private static final Map<String, StripedBufferPool> pools = new HashMap<>();

    private RecordsPool() {

    }

    public static void configure(PoolConfig config) {
        bufferRecordsCache.put(config.poolName, new ArrayDeque<>());
        channelRecordsCache.put(config.poolName, new ArrayDeque<>());
        segmentRecordsCache.put(config.poolName, new ArrayDeque<>());

        poolConfig.put(config.poolName, config);

        StripedBufferPool pool = new StripedBufferPool(config.maxRecordSize, config.directBuffers, config.poolStripes);
        pools.put(config.poolName, pool);

    }

    public static BufferRecords fromBuffer(String name, ByteBuffer data) {
        StripedBufferPool pool = pools.get(name);
        BufferRecords records = getBufferRecords(name);
        int batchSize = poolConfig.get(name).batchSize;

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

    public static AbstractChannelRecords fromChannel(String name, ReadableByteChannel channel) {
        StripedBufferPool pool = pools.get(name);
        PoolConfig config = RecordsPool.poolConfig.get(name);

        ChannelRecords records = getChannelRecords(name);
        records.init(config.readBufferSize, pool, channel);
        return records;
    }

    public static SegmentRecords fromSegment(String name, IndexedSegment segment, long startPos) {
        StripedBufferPool pool = pools.get(name);
        PoolConfig config = RecordsPool.poolConfig.get(name);

        SegmentRecords records = getSegmentRecords(name);
        records.init(config.readBufferSize, pool, segment, startPos);
        return records;
    }

    private static BufferRecords getBufferRecords(String name) {
        var queue = bufferRecordsCache.get(name);
        if (queue == null) {
            throw new IllegalArgumentException("No record cache for " + name);
        }
        BufferRecords records = queue.poll();
        if (records == null) {
            StripedBufferPool pool = pools.get(name);
            PoolConfig config = RecordsPool.poolConfig.get(name);
            return new BufferRecords(name, config.rowKey, config.batchSize, pool);
        }
        return records;
    }

    private static ChannelRecords getChannelRecords(String name) {
        var queue = channelRecordsCache.get(name);
        if (queue == null) {
            throw new IllegalArgumentException("No record cache for " + name);
        }
        ChannelRecords records = queue.poll();
        if (records == null) {
            return new ChannelRecords(name);
        }
        return records;
    }

    private static SegmentRecords getSegmentRecords(String name) {
        var queue = segmentRecordsCache.get(name);
        if (queue == null) {
            throw new IllegalArgumentException("No record cache for " + name);
        }
        SegmentRecords records = queue.poll();
        if (records == null) {
            return new SegmentRecords(name);
        }
        return records;
    }


    static void free(Records records) {
        if (records == null) {
            return;
        }
        if (records instanceof BufferRecords) {
            bufferRecordsCache.get(records.poolName()).offer((BufferRecords) records);
        } else if (records instanceof ChannelRecords) {
            channelRecordsCache.get(records.poolName()).offer((ChannelRecords) records);
        } else if (records instanceof SegmentRecords) {
            segmentRecordsCache.get(records.poolName()).offer((SegmentRecords) records);
        } else {
            throw new RuntimeException("Unknown type: " + records.getClass());
        }
    }


}
