package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class ShortField extends Field {

    private static final Mapper LEN_MAPPER = b -> Short.BYTES;

    public ShortField(int offset) {
        super(b -> offset, LEN_MAPPER);
    }

    private ShortField(Mapper offset, Mapper len) {
        super(offset, len);
    }

    public static ShortField after(Field field) {
        return new ShortField(b -> afterOf(field, b), LEN_MAPPER);
    }

    public int set(ByteBuffer b, short val) {
        int _offset = offset.apply(b);
        b.putShort(_offset, val);
        return Short.BYTES;
    }

    public short get(ByteBuffer b) {
        int _offset = this.offset.apply(b);
        return b.getShort(relativePosition(b, _offset));
    }
}
