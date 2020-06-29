package io.joshworks.ilog;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class Records implements Iterator<Record2> {

    private static final Queue<Records> cache = new ArrayBlockingQueue<>(1000);

    private final ByteBuffer data = ByteBuffer.allocate(4096);
    private final int[] offsets;
    private final int[] lengths;
    private int size;
    private int idx;

    private Records(int batchSize) {
        offsets = new int[batchSize];
        lengths = new int[batchSize];
    }

    public static Records from(ByteBuffer data) {
        Records records = cache.poll();
        records.idx = 0;
        records.size = 0;

        int remaining = data.remaining();
        if (remaining < HEADER_BYTES) {
            return records;
        }
        int rsize = Record.sizeOf(records);
        boolean hasReadableBytes = rsize <= remaining && rsize > HEADER_BYTES;
        return hasReadableBytes && Record.isValid(records);

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Record2 next() {
        return null;
    }
}
