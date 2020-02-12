package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class IntField extends Field {

    public IntField(IntField offsetSupplier) {
        super(offsetSupplier::get);
    }

    public IntField(int offset) {
        super(bb -> offset);
    }

    private IntField(Mapper offset) {
        super(offset);
    }

    public static IntField after(Field field) {
        return new IntField(b -> afterOf(field, b));
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

    @Override
    public int len(ByteBuffer b) {
        return Integer.BYTES;
    }
}
