package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class LongField extends Field{

    public LongField(int offset) {
        super(b -> offset, b -> Long.BYTES);
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
