package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class FloatField extends Field {

    private static final Mapper LEN_MAPPER = b -> Float.BYTES;

    public FloatField(int offset) {
        super(b -> offset, LEN_MAPPER);
    }

    private FloatField(Mapper offset, Mapper len) {
        super(offset, len);
    }

    public static FloatField after(Field field) {
        return new FloatField(b -> afterOf(field, b), LEN_MAPPER);
    }

    public float set(ByteBuffer b, float val) {
        int _offset = offset.apply(b);
        b.putFloat(_offset, val);
        return Float.BYTES;
    }

    public float get(ByteBuffer b) {
        int _offset = this.offset.apply(b);
        return b.getFloat(relativePosition(b, _offset));
    }
}
