package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public class VStringSerializer implements Serializer<String> {

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

    public static byte[] toBytes(String value) {
        return requireNonNull(value).getBytes(StandardCharsets.UTF_8);
    }

}
