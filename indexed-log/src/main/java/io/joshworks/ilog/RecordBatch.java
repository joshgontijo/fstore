package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

import static io.joshworks.ilog.Record2.HEADER_BYTES;
import static io.joshworks.ilog.Record2.recordSize;

public class RecordBatch {

    public static boolean hasNext(ByteBuffer record) {
        int remaining = record.remaining();
        if (remaining < HEADER_BYTES) {
            return false;
        }
        int rsize = recordSize(record);
        return rsize <= remaining;
    }

    public static void skip(ByteBuffer record) {
        if (!hasNext(record)) {
            return;
        }
        int recordSize = recordSize(record);
        Buffers.offsetPosition(record, recordSize);
    }

    public static int countRecords(ByteBuffer record) {
        int entries = 0;
        int offset = 0;
        while (true) {
            int remaining = record.remaining() - offset;
            if (remaining < HEADER_BYTES) {
                return entries;
            }
            int rsize = recordSize(record);
            if (rsize > remaining) {
                return entries;
            }
            entries++;
            offset += rsize;
        }

    }

}
