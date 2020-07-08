package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordUtils;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


public class RecordBatch {

    public static boolean hasNext(ByteBuffer records) {
        int remaining = records.remaining();
        if (remaining < Record2.HEADER_BYTES) {
            return false;
        }
        int rsize = RecordUtils.sizeOf(records);
        boolean hasReadableBytes = rsize <= remaining && rsize > Record2.HEADER_BYTES;
        return hasReadableBytes && RecordUtils.isValid(records);
    }

    public static void advance(ByteBuffer record) {
        if (!hasNext(record)) {
            return;
        }
        int recordSize = RecordUtils.sizeOf(record);
        Buffers.offsetPosition(record, recordSize);
    }

    public static int totalSize(ByteBuffer records) {
        int ppos = records.position();
        int plim = records.limit();

        int size = 0;
        while (hasNext(records)) {
            advance(records);
            size += RecordUtils.sizeOf(records);
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
