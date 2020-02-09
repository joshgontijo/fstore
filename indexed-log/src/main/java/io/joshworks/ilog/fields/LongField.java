package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class LongField extends Field {

    private static final Mapper LEN_MAPPER = b -> Long.BYTES;

    public LongField(int offset) {
        super(b -> offset, LEN_MAPPER);
    }

    private LongField(Mapper offset, Mapper len) {
        super(offset, len);
    }

    public static LongField after(Field field) {
        return new LongField(b -> afterOf(field, b), LEN_MAPPER);
    }

    public int set(ByteBuffer b, long val) {
        int _offset = offset.apply(b);
        b.putLong(_offset, val);
        return Long.BYTES;
    }

    public long get(ByteBuffer b) {
        int _offset = this.offset.apply(b);
        return b.getLong(relativePosition(b, _offset));
    }
}
