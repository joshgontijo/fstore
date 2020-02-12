package io.joshworks.ilog.fields;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

/**
 * Integer prefixed field
 */
public class VarLenField extends Field {

    static final int HEADER_BYTES = Integer.BYTES;

    public VarLenField(int offset) {
        this(b -> offset);
    }

    public VarLenField(Mapper offset) {
        super(offset);
    }

    public static VarLenField after(Field field) {
        return new VarLenField(b -> afterOf(field, b));
    }

    @Override
    public int len(ByteBuffer b) {
        return HEADER_BYTES + b.getInt(b.position());
    }

    public int valueLen(ByteBuffer b) {
        return len(b) - HEADER_BYTES;
    }

    public int copyValueTo(ByteBuffer fieldBuffer, ByteBuffer dst) {
        int pos = pos(fieldBuffer);
        int len = len(fieldBuffer) - HEADER_BYTES;
        return Buffers.copy(fieldBuffer, pos + HEADER_BYTES, len, dst);
    }

    public int set(ByteBuffer fieldBuffer, ByteBuffer value) {
        int pos = pos(fieldBuffer);
        int valueLen = value.remaining();
        fieldBuffer.putInt(pos, valueLen);
        //_len => copy only the fields size to this buffer to avoid overflow;
        return Buffers.copy(value, value.position(), valueLen, fieldBuffer, pos + HEADER_BYTES);
    }
}
