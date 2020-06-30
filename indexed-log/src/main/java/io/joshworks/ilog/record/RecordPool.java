package io.joshworks.ilog.record;

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


    private static final Map<String, Queue<Records>> cache = new HashMap<>();
    private static final Map<String, StripedBufferPool> pools = new HashMap<>();
    private static final Map<String, Integer> keySizes = new HashMap<>();

    private RecordPool() {

    }

    public static void configure(String name, int keySize, int maxRecordSize, int maxRecords, StripedBufferPool bufferPool) {
        cache.put(name, new ArrayDeque<>(1000));
        pools.put(name, bufferPool);
        keySizes.put(name, keySize);
    }


    public static Records get(String name) {
        var queue = cache.get(name);
        if (queue == null) {
            throw new IllegalArgumentException("No record cache for " + name);
        }
        Records records = queue.poll();
        if (records == null) {
            StripedBufferPool pool = pools.get(name);
            int keySize = keySizes.get(name);
            return new Records(name, keySize, pool);
        }
        return records;
    }

    static void free(Records records) {
        cache.get(records.cachePoolName).offer(records);
    }


}
