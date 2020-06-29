package io.joshworks.ilog.pooled;

import io.joshworks.ilog.fields.ByteField;
import io.joshworks.ilog.fields.IntField;
import io.joshworks.ilog.fields.LongField;

import java.nio.ByteBuffer;

public class Record2 {

    private Record2() {

    }

    public static final IntField RECORD_LEN = new IntField(0);
    public static final IntField VALUE_LEN = IntField.after(RECORD_LEN);
    public static final IntField KEY_LEN = IntField.after(VALUE_LEN);
    public static final IntField CHECKSUM = IntField.after(KEY_LEN);
    public static final LongField TIMESTAMP = LongField.after(CHECKSUM);
    public static final ByteField ATTRIBUTE = ByteField.after(TIMESTAMP);

    public int length;
    public int valueLength;
    public int keyLength;
    public int checksum;
    public long timestamp;
    private ByteBuffer key;
    private ByteBuffer value;

    public boolean hasAttribute(int attr) {
        return false;
    }

    public void read(ByteBuffer buffer) {

    }

}
