package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class RecordBatch {

    public static boolean hasNext(ByteBuffer record) {
        int remaining = record.remaining();
        if (remaining < HEADER_BYTES) {
            return false;
        }
        int rsize = Record.sizeOf(record);
        boolean hasReadableBytes = rsize <= remaining && rsize > HEADER_BYTES;
        return hasReadableBytes && Record.isValid(record);
    }

    public static void advance(ByteBuffer record) {
        if (!hasNext(record)) {
            return;
        }
        int recordSize = Record.sizeOf(record);
        Buffers.offsetPosition(record, recordSize);
    }

    public static int countRecords(ByteBuffer record) {
        int ppos = record.position();
        int plim = record.limit();

        int entries = 0;
        while (hasNext(record)) {
            advance(record);
            entries++;
        }
        record.limit(plim).position(ppos);
        return entries;
    }

}
