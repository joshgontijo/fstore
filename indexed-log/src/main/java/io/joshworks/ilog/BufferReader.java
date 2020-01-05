package io.joshworks.ilog;

import java.nio.ByteBuffer;

/**
 * Absolute buffer reader, used to prevent source buffer to change its internal position, used for reads
 */
public class BufferReader {

    private final ByteBuffer buffer;
    private final int startPos;
    private int offset;

    public BufferReader(ByteBuffer buffer, int startPos) {
        this.buffer = buffer;
        this.startPos = startPos;
    }

    public long getLong() {
        long v = buffer.getLong(startPos + offset);
        offset += Long.BYTES;
        return v;
    }

    public int getInt() {
        int v = buffer.getInt(startPos + offset);
        offset += Integer.BYTES;
        return v;
    }

    public byte getByte() {
        byte v = buffer.get(startPos + offset);
        offset += Byte.BYTES;
        return v;
    }

    public short getShort() {
        short v = buffer.getShort(startPos + offset);
        offset += Short.BYTES;
        return v;
    }

    public float getFloat() {
        float v = buffer.getFloat(startPos + offset);
        offset += Float.BYTES;
        return v;
    }

    public double getDouble() {
        double v = buffer.getDouble(startPos + offset);
        offset += Double.BYTES;
        return v;
    }

}
