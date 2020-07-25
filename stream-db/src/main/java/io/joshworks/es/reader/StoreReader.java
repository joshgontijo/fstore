package io.joshworks.es.reader;

import io.joshworks.es.index.Index;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

public class StoreReader {

    private static final ThreadLocal<QueryPlanner> plannerCache = ThreadLocal.withInitial(QueryPlanner::new);
    private static final ThreadLocal<ByteBuffer> pageBufferCache = ThreadLocal.withInitial(() -> Buffers.allocate(4096, false));

    private final Log log;
    private final Index index;

    public StoreReader(Log log, Index index) {
        this.index = index;
        this.log = log;
    }

    public int get(long stream, int version, int maxEntries, ByteBuffer dst) {
        QueryPlanner planner = plannerCache.get();
        ByteBuffer pageBuffer = pageBufferCache.get();
        boolean success = planner.prepare(index, stream, version, maxEntries, pageBuffer);
        if (!success) {
            return 0;
        }

        return planner.execute(log, dst);
    }

}
