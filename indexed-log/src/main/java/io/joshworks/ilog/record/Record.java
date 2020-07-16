package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.index.RowKey;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * <pre>
 * RECORD_LEN       (4 BYTES)
 * CHECKSUM         (4 BYTES)
 * -------------------
 * TIMESTAMP        (8 BYTES)
 * SEQUENCE         (8 BYTES)
 * ATTRIBUTES       (2 BYTES)
 * KEY_LEN          (2 BYTES)
 * [KEY]            (N BYTES)
 * VALUE_LEN        (4 BYTES)
 * [VALUE]          (N BYTES)
 * </pre>
 * <p>
 *
 * <pre>
 * All records starts at position zero
 * Checksum covers from TIMESTAMP inclusive
 * RECORD_LEN includes RECORD_LEN itself
 * KEY_LEN excludes KEY_LEN itself
 * VALUE_LEN excludes VALUE_LEN itself
 * </pre>
 */
public class Record implements Closeable {

    private ByteBuffer data;

    static final int HEADER_BYTES =
            Integer.BYTES +         //RECORD_LEN
                    Integer.BYTES + //CHECKSUM
                    Long.BYTES +    //TIMESTAMP
                    Long.BYTES +    //SEQUENCE
                    Short.BYTES +   //ATTRIBUTES
                    Short.BYTES +   //KEY_LEN
                    Integer.BYTES;  //VALUE_LEN

    static final int RECORD_LEN_OFFSET = 0;
    static final int CHECKSUM_OFFSET = RECORD_LEN_OFFSET + Integer.BYTES;
    static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    static final int SEQUENCE_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;
    static final int ATTRIBUTE_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    static final int KEY_LEN_OFFSET = ATTRIBUTE_OFFSET + Short.BYTES;
    static final int KEY_OFFSET = KEY_LEN_OFFSET + Short.BYTES;

    boolean active;
    private final RecordPool owner;

    Record(RecordPool owner) {
        this.owner = owner;
    }

    void init(ByteBuffer buffer) {
        if(active) {
            throw new RuntimeException("Record is active");
        }
        assert isValid(buffer);

        this.data = buffer;
        this.active = true;
    }

    @Override
    public void close() {
        ByteBuffer tmp = data;
        data = null;
        active = false;
        if (owner != null) {
            owner.free(tmp);
            owner.free(this);
        }

    }

    //Returns the total size in bytes of this record, including RECORD_LEN field
    public static int recordSize(ByteBuffer recData) {
        return recordSize(recData, recData.position());
    }

    public static int recordSize(ByteBuffer recData, int offset) {
        return recData.getInt(offset + RECORD_LEN_OFFSET);
    }

    public boolean isValid() {
        return isValid(data);
    }

    public static boolean hasHeaderData(ByteBuffer recData) {
        return hasHeaderData(recData, recData.position());
    }

    public static boolean hasHeaderData(ByteBuffer recData, int offset) {
        return Buffers.remaining(recData, offset) >= Record.HEADER_BYTES;
    }

    static boolean isValid(ByteBuffer recData, int offset) {
        if (!hasHeaderData(recData, offset)) {
            return false;
        }

        int recSize = recordSize(recData, offset);
        if (recSize <= 0 || recSize > Buffers.remaining(recData, offset)) {
            return false;
//            SOMETHING WRONG WITH SEGMENTITERATORHERE -PUT A BP AND TEST append_MANY_TEST
        }

        int chksOffset = offset + Record.TIMESTAMP_OFFSET; //from TIMESTAMP
        int chksLen = recSize - (Integer.BYTES * 2);

        int checksum = recData.getInt(offset + Record.CHECKSUM_OFFSET);
        int computed = ByteBufferChecksum.crc32(recData, chksOffset, chksLen);

        return computed == checksum;
    }

    static boolean isValid(ByteBuffer recData) {
        if (recData.remaining() < Record.HEADER_BYTES) {
            return false;
        }

        int base = recData.position();
        int recSize = recordSize(recData);
        int checksum = recData.getInt(base + Record.CHECKSUM_OFFSET);

        if (recSize <= 0 || recSize > recData.remaining()) {
            return false;
        }

        int chksOffset = base + Record.TIMESTAMP_OFFSET; //from TIMESTAMP
        int chksLen = recSize - (Integer.BYTES * 2);

        int computed = ByteBufferChecksum.crc32(recData, chksOffset, chksLen);

        return computed == checksum;
    }

    public boolean hasAttribute(int attribute) {
        short attr = attributes();
        return (attr & (1 << attribute)) == 1;
    }

    public short attributes() {
        return data.getShort(ATTRIBUTE_OFFSET);
    }

    public int recordSize() {
        return data.getInt(RECORD_LEN_OFFSET); //not the same as 'static int recordSize' due to relative position
    }

    public long sequence() {
        return data.getLong(SEQUENCE_OFFSET);
    }

    public long timestamp() {
        return data.getLong(TIMESTAMP_OFFSET);
    }

    public short keyLen() {
        return data.getShort(KEY_LEN_OFFSET);
    }

    private int valueLenOffset() {
        return KEY_OFFSET + keyLen();
    }

    private int valueOffset() {
        return valueLenOffset() + Integer.BYTES;
    }

    public int valueSize() {
        return data.getInt(valueLenOffset());
    }

    public int checksum() {
        return data.getInt(CHECKSUM_OFFSET);
    }

    public void writeSequence(long sequence) {
        data.putLong(SEQUENCE_OFFSET, sequence);
    }

    private void create(ByteBuffer dst, ByteBuffer key, ByteBuffer value, int... attr) {
        create(dst, key, key.position(), key.remaining(), value, value.position(), value.remaining(), attr);
    }

    void create(ByteBuffer dst, ByteBuffer key, int kOffset, int kLen, ByteBuffer value, int vOffset, int vLen, int... attr) {
        if (data != null) {
            throw new IllegalStateException("Cannot overwrite record");
        }
        int recordSize = computeRecordSize(key.remaining(), value.remaining()); //includes the RECORD_LEN field
        if (recordSize > dst.remaining()) {
            throw new IllegalArgumentException("Not enough buffer space to create record");
        }
        if (kLen > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Key length exceeds mac size of: " + Short.MAX_VALUE);
        }

        dst.putInt(recordSize);                     // RECORD_LEN
        dst.putInt(0);                              // CHECKSUM (tmp)
        dst.putLong(System.currentTimeMillis());    // TIMESTAMP
        dst.putLong(0);                             // SEQUENCE (tmp)
        dst.putShort(attribute(attr));              // ATTRIBUTES
        dst.putShort((short) kLen);                 // KEY_LEN
        Buffers.copy(key, kOffset, kLen, dst);      // [KEY]
        dst.putInt(vLen);                           // VALUE_LEN
        Buffers.copy(value, vOffset, vLen, dst);    // [VALUE]

        dst.flip();

        assert dst.remaining() == recordSize;
        assert recordSize(dst) == recordSize;

        int checksum = ByteBufferChecksum.crc32(dst, TIMESTAMP_OFFSET, dst.remaining() - (Integer.BYTES * 2));
        dst.putInt(CHECKSUM_OFFSET, checksum);

        init(dst);
    }

    public static Record create(ByteBuffer key, ByteBuffer value, int... attr) {
        int recordSize = computeRecordSize(key.remaining(), value.remaining());
        ByteBuffer dst = Buffers.allocate(recordSize, false);
        Record rec = new Record(null);
        rec.create(dst, key, value, attr);
        return rec;
    }

    //computes the total record size including the RECORD_LEN field
    static int computeRecordSize(int kLen, int vLen) {
        return HEADER_BYTES + kLen + vLen;
    }

    private static short attribute(int... attributes) {
        short b = 0;
        for (int attr : attributes) {
            b = (short) (b | 1 << attr);
        }
        return b;
    }

    public int copyKey(ByteBuffer dst) {
        return Buffers.copy(data, KEY_OFFSET, keyLen(), dst);
    }

    public void copyValue(ByteBuffer dst) {
        Buffers.copy(data, valueOffset(), valueSize(), dst);
    }

    public int copyTo(ByteBuffer dst) {
        int recLen = recordSize();
        int copied = Buffers.copy(data, 0, recLen, dst);
        assert copied == recLen;
        return copied;
    }

    public int compare(RowKey rowKey, ByteBuffer key) {
        return rowKey.compare(data, KEY_OFFSET, key, key.position());
    }

    public int compare(RowKey rowKey, Record rec) {
        return rowKey.compare(data, KEY_OFFSET, rec.data, KEY_OFFSET);
    }

    public void reset() {
        int recSize = recordSize();
        data.position(0).limit(recSize);

    }
}
