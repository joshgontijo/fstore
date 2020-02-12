package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class ShortField extends Field {

    public ShortField(int offset) {
        super(b -> offset);
    }

    private ShortField(Mapper offset) {
        super(offset);
    }

    public static ShortField after(Field field) {
        return new ShortField(b -> afterOf(field, b));
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

    @Override
    public int len(ByteBuffer b) {
        return Short.BYTES;
    }
}
