package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import static io.joshworks.ilog.Record.HEADER_BYTES;

public class RecordBatch {

    public static boolean hasNext(ByteBuffer records) {
        int remaining = records.remaining();
        if (remaining < HEADER_BYTES) {
            return false;
        }
        int rsize = Record.sizeOf(records);
        boolean hasReadableBytes = rsize <= remaining && rsize > HEADER_BYTES;
        return hasReadableBytes && Record.isValid(records);
    }

    public static void advance(ByteBuffer record) {
        if (!hasNext(record)) {
            return;
        }
        int recordSize = Record.sizeOf(record);
        Buffers.offsetPosition(record, recordSize);
    }

    public static int totalSize(ByteBuffer records) {
        int ppos = records.position();
        int plim = records.limit();

        int size = 0;
        while (hasNext(records)) {
            advance(records);
            size += Record.sizeOf(records);
        }
        records.limit(plim).position(ppos);
        return size;
    }

    public static int countRecords(ByteBuffer records) {
        int ppos = records.position();
        int plim = records.limit();

        int entries = 0;
        while (hasNext(records)) {
            advance(records);
            entries++;
        }
        records.limit(plim).position(ppos);
        return entries;
    }

    public static int writeTo(ByteBuffer records, WritableByteChannel channel) {
        try {
            int startPos = records.position();
            int plim = records.limit();
            int endPos = startPos;
            while (RecordBatch.hasNext(records)) {
                RecordBatch.advance(records);
                endPos = records.position();
            }
            records.limit(endPos).position(startPos);
            int written = channel.write(records);
            records.limit(plim).position(endPos);
            return written;
        } catch (Exception e) {
            throw new RuntimeIOException(e);
        }
    }
}
