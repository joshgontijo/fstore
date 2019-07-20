package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class VStringSerializer implements Serializer<String> {

    @Override
    public ByteBuffer toBytes(String data) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES + bytes.length);
        return bb.putInt(bytes.length).put(bytes).flip();
    }

    @Override
    public void writeTo(String data, ByteBuffer dst) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        dst.putInt(bytes.length).put(bytes);
    }

    @Override
    public String fromBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (!buffer.hasArray()) {
            byte[] data = new byte[length];
            buffer.get(data);
            return new String(data, StandardCharsets.UTF_8);
        }

        String value = new String(buffer.array(), buffer.position(), length, StandardCharsets.UTF_8);
        buffer.position(buffer.position() + length);
        return value;
    }

    public static int sizeOf(String value) {
        if (value == null) {
            return Integer.BYTES;
        }
        return value.length() + Integer.BYTES;
    }

    //Iterates over a array of VSTRING, the array must containg only VSTRING ENTRIES
    public static Iterator<String> iterator(ByteBuffer data) {
        return new Iterator<>() {

            private final Serializer<String> instance = new VStringSerializer();

            @Override
            public boolean hasNext() {
                int pos = data.position();
                int nextLength = data.getInt(pos);
                int remaining = data.remaining();
                return data.hasRemaining() && remaining > Integer.BYTES + nextLength;
            }

            @Override
            public String next() {
                if (hasNext()) {
                    throw new NoSuchElementException();
                }
                return instance.fromBytes(data);
            }
        };
    }


//    @Override
//    public String fromBytes(ByteBuffer buffer) {
//        int length = buffer.getInt();
//        int limit = buffer.limit();
//        buffer.limit(buffer.position() + length);
//        String data = StandardCharsets.UTF_8.decode(buffer).toString();
//        buffer.limit(limit);
//        return data;
//    }

}
