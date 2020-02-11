package io.joshworks.ilog.fields;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

/**
 * Array of fixed size fields
 * ENTRY_COUNT
 * ENTRY_SIZE
 * //entries
 */
public class ArrayField extends Field {

    static final int HEADER_BYTES = Integer.BYTES * 2;
    private final int fieldLength;

    //only fixed size can be used
    private ArrayField(Mapper offset, Mapper len) {
        super(offset, b -> computeArraySize(offset, b));
    }

    public ArrayField(Mapper offset, int fieldLength) {
        super(offset, b -> computeArraySize(offset, b));
        this.fieldLength = fieldLength;
    }

    public static ArrayField after(Field field, int fieldLength) {
        if (fieldLength <= 0) {
            throw new IllegalStateException("Field length must be greater than zero");
        }
        Mapper offset = b -> afterOf(field, b);
        return new ArrayField(offset, b -> computeArraySize(offset, b));
    }

    public static int computeArraySize(Mapper offset, ByteBuffer fieldBuffer) {
        int _offset = offset.apply(fieldBuffer);
        int entryCount = fieldBuffer.getInt(_offset);
        int elementSize = fieldBuffer.getInt(_offset + Integer.BYTES);
        return entryCount * elementSize;
    }

//    //returns the offset o f the field in the array
//    public int get(ByteBuffer fieldBuffer, int idx) {
//        int entries = entries(fieldBuffer);
//        if (idx >= entries) {
//            throw new IndexOutOfBoundsException(idx + ">" + (entries - 1));
//        }
//        int startPos = startPos(fieldBuffer, idx);
//        int entryPos = relativePosition(fieldBuffer, startPos);
//        return fieldBuffer.getInt(entryPos);
//    }

    public int len(ByteBuffer b) {
        return HEADER_BYTES + entries(b) * b.getInt(b.position());
    }

    public int add(ByteBuffer fieldBuffer, ByteBuffer src, int idx) {
        int pos = pos(fieldBuffer);
        int entrySize = fieldBuffer.getInt()
    }

    public int entries(ByteBuffer b) {
        int fieldLength = len(b);
        return b.getInt(b.position() + fieldLength);
    }

    private int startPos(ByteBuffer fieldBuffer, int idx) {
        int _fieldLen = len(fieldBuffer);
        int _offset = offset.apply(fieldBuffer);
        return _offset + (_fieldLen * idx);
    }

}
