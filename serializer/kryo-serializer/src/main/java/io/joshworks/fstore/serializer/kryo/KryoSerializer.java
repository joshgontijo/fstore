package io.joshworks.fstore.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.joshworks.fstore.core.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class KryoSerializer {

    private static final ThreadLocal<Kryo> localKryo = ThreadLocal.withInitial(DefaultInstance::newKryoInstance);

    public static <T> Serializer<T> serializerOf(Class<T> type) {
        return new StaticSerializer<>();
    }

    public static byte[] serialize(Object data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializeInternal(data, null, baos);
        return baos.toByteArray();
    }

    public static byte[] serialize(Object data, Class<?> type) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializeInternal(data, type, baos);
        return baos.toByteArray();
    }

    public static void serialize(Object data, ByteBuffer buffer) {
        serialize(data, buffer, null);
    }

    public static void serialize(Object data, ByteBuffer buffer, Class<?> type) {
        OutputStream baos = new ByteBufferOutputStream(buffer);
        serializeInternal(data, type, baos);
    }

    private static void serializeInternal(Object data, Class<?> type, OutputStream baos) {
        Kryo kryo = localKryo.get();
        try (Output output = new Output(baos)) {
            if (type != null) {
                kryo.writeObject(output, data);
            } else {
                kryo.writeClassAndObject(output, data);
            }
        }
    }

    public static <T> T deserialize(byte[] data) {
        return deserialize(data, null);
    }

    public static <T> T deserialize(byte[] data, Class<T> type) {
        Kryo kryo = localKryo.get();
        InputStream inputStream = new ByteArrayInputStream(data);
        try (Input input = new Input(inputStream)) {
            if (type != null) {
                return kryo.readObject(input, type);
            }
            return (T) kryo.readClassAndObject(input);
        }
    }

    public static <T> T deserialize(ByteBuffer data) {
        return deserialize(data, null);
    }

    public static <T> T deserialize(ByteBuffer data, Class<T> type) {
        Kryo kryo = localKryo.get();
        InputStream inputStream = new ByteBufferInputStream(data);
        try (Input input = new Input(inputStream)) {
            if (type != null) {
                return kryo.readObject(input, type);
            }
            return (T) kryo.readClassAndObject(input);
        }
    }

    private static class StaticSerializer<T> implements Serializer<T> {

        @Override
        public void writeTo(T data, ByteBuffer dst) {
            KryoSerializer.serialize(data, dst);
        }

        @Override
        public T fromBytes(ByteBuffer buffer) {
            return KryoSerializer.deserialize(buffer);
        }
    }

}
