package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class DoubleField extends Field {

    private static final Mapper LEN_MAPPER = b -> Double.BYTES;

    public DoubleField(int offset) {
        super(b -> offset, LEN_MAPPER);
    }

    private DoubleField(Mapper offset, Mapper len) {
        super(offset, len);
    }

    public static DoubleField after(Field field) {
        return new DoubleField(b -> afterOf(field, b), LEN_MAPPER);
    }

    public double set(ByteBuffer b, float val) {
        int _offset = offset.apply(b);
        b.putDouble(_offset, val);
        return Double.BYTES;
    }

    public double get(ByteBuffer b) {
        int _offset = this.offset.apply(b);
        return b.getDouble(relativePosition(b, _offset));
    }
}
