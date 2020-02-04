package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record2;

import java.nio.ByteBuffer;

/**
 * KEY_LEN (4 bytes)
 * KEY (N bytes)
 * VAL_LEN (4 BYTES)
 * TIMESTAMP (8 bytes)
 * ATTRIBUTE (1 byte)
 * VAL (N Bytes)
 */
public class LsmRecord {

    public static final int DELETION_ATTR = 0;
    private static final int HAS_MAX_AGE = 1 << 1;

    private static final int KEY_OFFSET = 0;

    //-------- TODO MOVE THESE TWO METHOS AS THE WRITE PATH USES RECORD INSTEAD LSMRECORD -------
    public static boolean deletion(ByteBuffer record) {
        return Record2.hasAttribute(record, DELETION_ATTR);
    }

    public static boolean expired(ByteBuffer record, long maxAgeSeconds) {
        boolean hasMaxAge = Record2.hasAttribute(record, HAS_MAX_AGE);
        if (!hasMaxAge) {
            return false;
        }
        long timestamp = Record2.timestamp(record);
        long now = nowSeconds();
        return maxAgeSeconds > 0 && (now - timestamp > maxAgeSeconds);
    }

    private static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    //-------------------------------------------------------------------

    public static int fromRecord(ByteBuffer record, ByteBuffer dst) {
        int spos = dst.position();
        int keySize = Record2.keySize(record);
        dst.putInt(keySize);
        Record2.writeKey(record, dst);
        dst.putInt(Record2.valueSize(record));
        dst.putLong(Record2.timestamp(record));
        dst.put(Record2.attributes(record));
        Record2.writeValue(record, dst);

        return dst.position() - spos;
    }

    public static int fromBlockRecord(ByteBuffer record, ByteBuffer decompressedBlock, ByteBuffer dst, int keyIdx, int keySize) {
        int valueOffset = Block2.entryOffset(record, keyIdx, keySize);
        Buffers.offsetPosition(decompressedBlock, valueOffset);

        int entryLen = Block2.Record.valueSize(decompressedBlock);
        if (entryLen <= 0) {
            throw new IllegalStateException("Invalid entry length");
        }

        dst.putInt(keySize);
        Block2.writeKey(record, dst, keyIdx, keySize);
        dst.putInt(entryLen);
        dst.putLong(Block2.Record.timestamp(decompressedBlock));
        dst.put(Block2.Record.attribute(decompressedBlock));
        Block2.Record.writeValue(decompressedBlock, valueOffset, entryLen, dst);

        //total lsmrecord len
        return keySize + Integer.BYTES + Long.BYTES + Byte.BYTES + entryLen;
    }


//
//    public static int valueSize(int keyLen, ByteBuffer record) {
//        return record.getInt(relativePosition(record, keyLen));
//    }
//
//    public static int writeKey(ByteBuffer record, ByteBuffer dst, int keyLen) {
//        int keyOffset = relativePosition(record, KEY_OFFSET);
//        return Buffers.copy(record, keyOffset, keyLen, dst);
//    }


}
