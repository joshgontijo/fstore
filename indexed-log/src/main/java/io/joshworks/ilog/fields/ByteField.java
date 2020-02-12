package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class ByteField extends Field {

    public ByteField(ByteField offsetSupplier) {
        super(offsetSupplier::get);
    }

    public ByteField(int offset) {
        super(bb -> offset);
    }

    private ByteField(Mapper offset) {
        super(offset);
    }

    public static ByteField after(Field field) {
        return new ByteField(b -> afterOf(field, b));
    }

    public int set(ByteBuffer b, byte val) {
        int _offset = offset.apply(b);
        b.put(_offset, val);
        return Byte.BYTES;
    }

    public byte get(ByteBuffer b) {
        int _offset = offset.apply(b);
        return b.get(relativePosition(b, _offset));
    }

    @Override
    public int len(ByteBuffer b) {
        return Byte.BYTES;
    }
}
