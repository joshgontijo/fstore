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

    public ArrayField(Mapper offset, int elementSize) {
        super(offset, b -> totalArraySize(offset, b, elementSize));
        this.elementSize = elementSize;
    }

    public static ArrayField after(Field field, int elementSize) {
        return new ArrayField(b -> afterOf(field, b), elementSize);
    }

    private static int totalArraySize(Mapper offset, ByteBuffer fieldBuffer, int elementSize) {
        int base = offset.apply(fieldBuffer);
        int entryCount = fieldBuffer.getInt(base);
        return entryCount * elementSize;
    }

    @Override
    public int len(ByteBuffer b) {
        return HEADER_BYTES + entries(b) * b.getInt(b.position());
    }

    public int add(ByteBuffer fieldBuffer, ByteBuffer src, int idx) {
        int base = pos(fieldBuffer);
        int entrySize = fieldBuffer.getInt(base);
        int entryPos = base + (idx * (HEADER_BYTES + entrySize));

        assert fieldBuffer.capacity() - entryPos >= src.remaining();
        return Buffers.copy(src, src.position(), elementSize, fieldBuffer, entryPos);
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
    }
}
