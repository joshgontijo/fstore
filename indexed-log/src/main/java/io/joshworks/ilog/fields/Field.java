package io.joshworks.ilog.fields;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public abstract class Field {

    protected final Mapper offset;

    public Field(Mapper offset) {
        this.offset = offset;
    }

    protected static int afterOf(Field field, ByteBuffer b) {
        int _offset = field.offset.apply(b);
        int _len = field.len(b);
        return _offset + _len;
    }

    public int offset(ByteBuffer b) {
        return offset.apply(b);
    }

    public int relativeOffset(ByteBuffer b) {
        return b.position() + offset.apply(b);
    }

    public abstract int len(ByteBuffer b);

    public int copyTo(ByteBuffer thisFieldBuffer, ByteBuffer value) {
        int _offset = pos(thisFieldBuffer);
        int _len = len(thisFieldBuffer);
        if (value.remaining() < _len) {
            throw new IllegalStateException("Expected at lease " + _len + " from the value buffer");
        }
        return Buffers.copy(thisFieldBuffer, _offset, _len, value);
    }

    public int copyTo(ByteBuffer thisFieldBuffer, ByteBuffer value, Field targetField) {
        int thisLen = len(thisFieldBuffer);
        int valueLen = targetField.len(value);
        if (thisLen != valueLen) {
            throw new IllegalArgumentException("Field length mismatch");
        }
        int thisBufferOffset = pos(thisFieldBuffer);

        int valueOffset = targetField.offset.apply(value);
        return Buffers.copy(thisFieldBuffer, thisBufferOffset, thisLen, value, valueOffset);
    }

    public int copyFrom(ByteBuffer thisFieldBuffer, ByteBuffer value) {
        int _offset = pos(thisFieldBuffer);
        int valueLen = value.remaining();
        if (thisFieldBuffer.limit() - _offset < valueLen) {
            throw new IllegalStateException("Expected at least " + valueLen + " from the value buffer");
        }
        //_len => copy only the fields size to this buffer to avoid overflow;
        return Buffers.copy(value, value.position(), valueLen, thisFieldBuffer, _offset);
    }

    public int copyFrom(ByteBuffer thisFieldBuffer, ByteBuffer value, Field srcField) {
        int thisLen = len(thisFieldBuffer);
        int valueLen = srcField.len(value);
        if (thisLen != valueLen) {
            throw new IllegalArgumentException("Field length mismatch");
        }

        int thisBufferOffset = pos(thisFieldBuffer);

        int valueOffset = srcField.offset.apply(value);
        return Buffers.copy(value, valueOffset, valueLen, thisFieldBuffer, thisBufferOffset);
    }

    protected int pos(ByteBuffer thisFieldBuffer) {
        int _offset = offset.apply(thisFieldBuffer);
        _offset = relativePosition(thisFieldBuffer, _offset);
        return _offset;
    }

}
