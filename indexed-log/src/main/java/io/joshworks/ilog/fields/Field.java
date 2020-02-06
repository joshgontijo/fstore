package io.joshworks.ilog.fields;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

public class Field {

    protected final Mapper offset;
    protected final Mapper len;

    public Field(Mapper offset, Mapper len) {
        this.offset = offset;
        this.len = len;
    }

    public int offset(ByteBuffer b) {
        return offset.apply(b);
    }

    public int len(ByteBuffer b) {
        return len.apply(b);
    }

    public int copyTo(ByteBuffer thisFieldBuffer, ByteBuffer value) {
        int _offset = offset.apply(thisFieldBuffer);
        _offset = relativePosition(thisFieldBuffer, _offset);
        int _len = len.apply(thisFieldBuffer);
        if (value.remaining() < _len) {
            throw new IllegalStateException("Expected at lease " + _len + " from the value buffer");
        }
        return Buffers.copy(thisFieldBuffer, _offset, _len, value);
    }

    public int copyTo(ByteBuffer thisFieldBuffer, ByteBuffer value, Field valueMapper) {
        int thisLen = len.apply(thisFieldBuffer);
        int valueLen = valueMapper.len.apply(value);
        if (thisLen != valueLen) {
            throw new IllegalArgumentException("Field length mismatch");
        }

        int thisBufferOffset = offset.apply(thisFieldBuffer);
        thisBufferOffset = relativePosition(thisFieldBuffer, thisBufferOffset);

        int valueOffset = valueMapper.offset.apply(value);
        return Buffers.copy(thisFieldBuffer, thisBufferOffset, thisLen, value, valueOffset);
    }

    public int copyFrom(ByteBuffer thisFieldBuffer, ByteBuffer value) {
        int _offset = offset.apply(thisFieldBuffer);
        _offset = relativePosition(thisFieldBuffer, _offset);
        int valueLen = value.remaining();
        if (thisFieldBuffer.limit() - _offset < valueLen) {
            throw new IllegalStateException("Expected at least " + valueLen + " from the value buffer");
        }
        //_len => copy only the fields size to this buffer to avoid overflow;
        return Buffers.copy(value, value.position(), valueLen, thisFieldBuffer, _offset);
    }

    public int copyFrom(ByteBuffer thisFieldBuffer, ByteBuffer value, Field valueMapper) {
        int thisLen = len.apply(thisFieldBuffer);
        int valueLen = valueMapper.len.apply(value);
        if (thisLen != valueLen) {
            throw new IllegalArgumentException("Field length mismatch");
        }

        int thisBufferOffset = offset.apply(thisFieldBuffer);
        thisBufferOffset = relativePosition(thisFieldBuffer, thisBufferOffset);

        int valueOffset = valueMapper.offset.apply(value);
        return Buffers.copy(value, valueOffset, valueLen, thisFieldBuffer, thisBufferOffset);
    }

}
