package io.joshworks.es;

import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Segment;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.MemTable;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.File;

public class EventStore {

    private final Log<Segment> log;
    private final File logRoot;
    private final RecordPool pool = RecordPool.create().build();
    private final RowKey rowKey = new StreamKey();

    private final MemTable memTable;

    private final Index index;

    public EventStore(File root) {
        this.logRoot = new File(root, "log");
//        int maxEntries = Buffers.MAX_CAPACITY / rowKey.keySize();
        int maxEntries = 1000;


        this.memTable = new MemTable(pool, rowKey, maxEntries);
        this.index = new Index(new File(root, "index.log"), maxEntries, rowKey);
        this.log = new Log<>(
                logRoot,
                Segment.NO_MAX_SIZE,
                -1,
                1,
                FlushMode.ON_ROLL,
                pool,
                Segment::new);
    }


    public void append(Records records) {
        log.append(records);
        RecordIterator it = records.iterator();
        while (it.hasNext()) {
            memTable.add(it);
            if (memTable.isFull()) {
                flush();
            }
        }
    }

    public synchronized void flush() {
        for (Node node : memTable) {
            index.write();
        }

    }

}
