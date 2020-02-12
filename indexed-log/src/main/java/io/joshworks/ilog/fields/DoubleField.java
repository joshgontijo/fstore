package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class DoubleField extends Field {

    public DoubleField(int offset) {
        super(b -> offset);
    }

    private DoubleField(Mapper offset) {
        super(offset);
    }

    public static DoubleField after(Field field) {
        return new DoubleField(b -> afterOf(field, b));
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

    @Override
    public int len(ByteBuffer b) {
        return Double.BYTES;
    }
}
