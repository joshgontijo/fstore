package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexKey;
import io.joshworks.eventry.index.IndexKeySerializer;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.streams.StreamHasher;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.Expression;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;

public class EStore {

    private static final String STREAM_PREFIX = "stream-";
    private static final int ITEMS = 15000000;
    private static final int STREAMS = 100000;
    private static final int THREADS = 1;
    public static final int FLUSH_THRESHOLD = 100000;
    private static LsmTree<IndexKey, EventRecord> store;

    private static final Metrics metrics = new Metrics();

    public static void main(String[] args) {
        File dir = new File("S:\\es-server-1");

        FileUtils.tryDelete(dir);

        store = LsmTree
                .builder(dir, new IndexKeySerializer(), new EventSerializer())
                .codec(new SnappyCodec())
                .flushThreshold(FLUSH_THRESHOLD)
                .bloomFilter(0.01, FLUSH_THRESHOLD)
                .blockCache(Cache.softCache())
                .sstableStorageMode(StorageMode.MMAP)
                .transacationLogStorageMode(StorageMode.MMAP)
                .blockSize(Size.KB.ofInt(4))
                .open();

        Thread monitor = new Thread(EStore::monitor);
        monitor.start();

        write();

//        CloseableIterator<Entry<IndexKey, EventRecord>> iterator = store.iterator(Direction.FORWARD);
//        int i = 0;
//        long s = System.currentTimeMillis();
//        while(iterator.hasNext()) {
//            Entry<IndexKey, EventRecord> next = iterator.next();
//            i++;
//        }
//        System.out.println("RREEEEEAD " + (System.currentTimeMillis() - s) + " ---- " + i);

        for (int i = 0; i < STREAMS * 10; i++) {
            try {
                String stream = STREAM_PREFIX + (i % STREAMS);
                List<EventRecord> events = events(stream);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static EventRecord add(String stream, Object data) {
        long hash = StreamHasher.hash(stream);
        int version = getVersion(hash) + 1;
        byte[] dataBytes = JsonSerializer.toBytes(data);
        byte[] metadataBytes = new byte[0];
        EventRecord record = new EventRecord(stream, "type-1", version, System.currentTimeMillis(), dataBytes, metadataBytes);

        store.put(IndexKey.event(hash, version), record);
        return record;
    }

    private static int getVersion(long stream) {
        IndexKey maxStreamVersion = IndexKey.event(stream, Integer.MAX_VALUE);
        Entry<IndexKey, EventRecord> found = store.find(maxStreamVersion, Expression.FLOOR, entry -> entry.stream == stream);
        return found != null ? found.key.version : NO_VERSION;
    }

//    //TODO broken
//    private static List<EventRecord> events(String stream) {
//        long start = System.currentTimeMillis();
//        List<EventRecord> records = Iterators.stream(store.iterator(Direction.FORWARD, IndexKey.allOf(StreamHasher.hash(stream))))
//                .map(kv -> kv.value)
//                .collect(Collectors.toList());
//        System.out.println("Completed in " + (System.currentTimeMillis() - start));
//        metrics.update("readStream");
//        metrics.update("readStreamPerSec");
//        metrics.update("totalEvents", records.size());
//
//        if (records.size() != ITEMS / STREAMS) {
//            throw new RuntimeException("Not expected: " + records.size());
//        }
//
//        return records;
//    }

    private static List<EventRecord> events(String stream) {
        List<EventRecord> records = new ArrayList<>();
        long hash = StreamHasher.hash(stream);
        int version = 0;
        EventRecord record;
        do {
            record = store.get(IndexKey.event(hash, version));
            if (record != null) {
                records.add(record);
            }
            version++;
        } while (record != null);
        metrics.update("readStream");
        metrics.update("readStreamPerSec");
        metrics.update("totalEvents", records.size());
        return records;
    }

    private static void write() {
        long s = System.currentTimeMillis();
        for (int i = 0; i < ITEMS; i++) {
            String stream = STREAM_PREFIX + (i % STREAMS);

            long start = System.currentTimeMillis();
            add(stream, new UserCreated("josh", i));
            metrics.update("writeTime", (System.currentTimeMillis() - start));
            metrics.update("writes");
            metrics.update("totalWrites");
        }
        System.out.println("WRITES: " + ITEMS + " IN " + (System.currentTimeMillis() - s));
    }

    private static void monitor() {
        while (true) {
            Long writes = metrics.remove("writes");
            Long reads = metrics.remove("reads");
            Long totalWrites = metrics.get("totalWrites");
            Long totalReads = metrics.get("totalReads");
            Long writeTime = metrics.get("writeTime");

            Long streamReads = metrics.get("readStream");
            Long totalEvents = metrics.get("totalEvents");
            Long readStreamPerSec = metrics.remove("readStreamPerSec");

            long avgWriteTime = totalWrites == null ? 0 : writeTime / totalWrites;
            System.out.println("WRITE: " + writes + "/s - TOTAL WRITE: " + totalWrites + " - AVG_WRITE_TIME: " + avgWriteTime + " READ: " + reads + "/s - TOTAL READ: " + totalReads + " STREAM_READ: " + streamReads + " TOTAL_EVENTS: " + totalEvents + " STREAM_READ_PER_SEC:" + readStreamPerSec);
            Threads.sleep(1000);
        }
    }

}