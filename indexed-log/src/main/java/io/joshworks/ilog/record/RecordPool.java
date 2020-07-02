package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class RecordPool {

    //new StripedBufferPool(Size.MB.ofInt(20), false,
    //            Size.BYTE.ofInt(512),
    //            Size.KB.ofInt(1),
    //            Size.KB.ofInt(2),
    //            Size.KB.ofInt(4),
    //            Size.KB.ofInt(8),
    //            Size.KB.ofInt(16),
    //            Size.KB.ofInt(32),
    //            Size.KB.ofInt(64),
    //            Size.KB.ofInt(256),
    //            Size.KB.ofInt(512),
    //            Size.MB.ofInt(1),
    //            Size.MB.ofInt(5)
    //    );


    //object cache
    private static final Map<String, Queue<BufferRecords>> bufferRecordsCache = new HashMap<>();
    private static final Map<String, Queue<ChannelRecords>> channelRecordsCache = new HashMap<>();
    private static final Map<String, Queue<SegmentRecords>> segmentRecordsCache = new HashMap<>();

    private static final Map<String, StripedBufferPool> pools = new HashMap<>();
    private static final Map<String, Integer> keySizes = new HashMap<>();

    private RecordPool() {

    }

    public static void configure(String name, int keySize, int maxRecordSize, int maxRecords, StripedBufferPool bufferPool) {
        bufferRecordsCache.put(name, new ArrayDeque<>());
        pools.put(name, bufferPool);
        keySizes.put(name, keySize);
    }

    public static BufferRecords fromBuffer(String name, ByteBuffer data) {
        StripedBufferPool pool = pools.get(name);
        BufferRecords records = bufferRecordsCache.get(name).poll();

        int i = 0;
        int copied = 0;
        while (RecordBatch.hasNext(data) && i < maxRecords) {
            int rsize = Record.sizeOf(data);
            ByteBuffer recData = pool.allocate(rsize);
            copied += Buffers.copy(data, data.position(), rsize, recData);

            records.add(recData);
            i++;
        }
        Buffers.offsetPosition(data, copied);
        return records;
    }

    public static ChannelRecords fromChannel(String name, int bufferSize, ReadableByteChannel channel) {
        StripedBufferPool pool = pools.get(name);
        ChannelRecords records = channelRecordsCache.get(name).poll();
        records.init(channel, bufferSize, pool);
        return records;
    }

    public static SegmentRecords fromSegment(String name, ReadableByteChannel channel) {
        StripedBufferPool pool = pools.get(name);
        ChannelRecords records = channelRecordsCache.get(name).poll();
        records.init(channel);
        return records;
    }

    public static BufferRecords get(String name) {
        var queue = bufferRecordsCache.get(name);
        if (queue == null) {
            throw new IllegalArgumentException("No record cache for " + name);
        }
        BufferRecords records = queue.poll();
        if (records == null) {
            StripedBufferPool pool = pools.get(name);
            int keySize = keySizes.get(name);
            return new BufferRecords(name, keySize, pool);
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
