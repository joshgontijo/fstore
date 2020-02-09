package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class IntField extends Field {

    private static final Mapper LEN_MAPPER = b -> Integer.BYTES;

    public IntField(IntField offsetSupplier) {
        super(offsetSupplier::get, LEN_MAPPER);
    }

    public IntField(int offset) {
        super(bb -> offset, LEN_MAPPER);
    }

    private IntField(Mapper offset, Mapper len) {
        super(offset, len);
    }

    public static IntField after(Field field) {
        return new IntField(b -> afterOf(field, b), LEN_MAPPER);
    }

    public int set(ByteBuffer b, int val) {
        int _offset = offset.apply(b);
        b.putInt(_offset, val);
        return Integer.BYTES;
    }

    public int get(ByteBuffer b) {
        int _offset = offset.apply(b);
        return b.getInt(relativePosition(b, _offset));
    }
}
