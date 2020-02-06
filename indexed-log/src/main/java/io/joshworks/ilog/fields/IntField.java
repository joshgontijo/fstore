package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class IntField extends Field {

    public IntField(IntField offsetSupplier) {
        super(offsetSupplier::get, bb -> Integer.BYTES);
    }

    public IntField(int offset) {
        super(bb -> offset, bb -> Integer.BYTES);
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
