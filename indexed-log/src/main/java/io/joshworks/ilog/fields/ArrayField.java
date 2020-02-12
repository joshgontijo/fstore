package io.joshworks.ilog.fields;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

/**
 * Array of fixed size fields
 * ENTRY_COUNT
 * ENTRY_SIZE
 * //entries
 */
public class ArrayField extends Field {

    static final int HEADER_BYTES = Integer.BYTES * 2;
    private final int elementSize;

    public ArrayField(int offset, int elementSize) {
        this(b -> offset, elementSize);
    }

    public ArrayField(Mapper offset, int elementSize) {
        super(offset);
        this.elementSize = elementSize;
    }

    public static ArrayField after(Field field, int elementSize) {
        return new ArrayField(b -> afterOf(field, b), elementSize);
    }

    @Override
    public int len(ByteBuffer b) {
        return HEADER_BYTES + entries(b) * valueLen();
    }

    public int add(ByteBuffer fieldBuffer, ByteBuffer src, int idx) {
        int base = pos(fieldBuffer);
        int entryPos = base + HEADER_BYTES + (idx * elementSize);

        assert fieldBuffer.capacity() - entryPos >= src.remaining();
        return Buffers.copy(src, src.position(), elementSize, fieldBuffer, entryPos);
    }

    public int add(ByteBuffer fieldBuffer, Field srcField, ByteBuffer src, int idx) {
        int base = pos(fieldBuffer);
        int entryPos = base + HEADER_BYTES + (idx * elementSize);

        int srcOffset = srcField.offset(src);
        int srcLen = srcField.offset(src);

        assert srcLen == elementSize;

        return Buffers.copy(src, srcOffset, elementSize, fieldBuffer, entryPos);
    }

    public int entries(ByteBuffer fieldBuffer) {
        int base = pos(fieldBuffer);
        return fieldBuffer.getInt(base);
    }

    public int valueLen() {
        return elementSize;
    }

    public int copyValueTo(ByteBuffer fieldBuffer, int idx, ByteBuffer dst) {
        int base = pos(fieldBuffer);
        int entryPos = base + HEADER_BYTES + (idx * elementSize);
        return Buffers.copy(fieldBuffer, entryPos, elementSize, dst);
    }

}
