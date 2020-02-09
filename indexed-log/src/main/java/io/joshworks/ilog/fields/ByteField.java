package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class ByteField extends Field {

    private static final Mapper LEN_MAPPER = b -> Byte.BYTES;

    public ByteField(ByteField offsetSupplier) {
        super(offsetSupplier::get, LEN_MAPPER);
    }

    public ByteField(int offset) {
        super(bb -> offset, LEN_MAPPER);
    }

    private ByteField(Mapper offset, Mapper len) {
        super(offset, len);
    }

    public static ByteField after(Field field) {
        return new ByteField(b -> afterOf(field, b), LEN_MAPPER);
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
}
