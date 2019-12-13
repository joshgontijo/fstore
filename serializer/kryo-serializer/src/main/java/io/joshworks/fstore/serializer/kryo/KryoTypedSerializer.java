package io.joshworks.fstore.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.joshworks.fstore.core.Serializer;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class KryoTypedSerializer<T> implements Serializer<T> {

    private static final ThreadLocal<Kryo> local = ThreadLocal.withInitial(DefaultInstance::newKryoInstance);

    @Override
    public void writeTo(T data, ByteBuffer dst) {
        Kryo kryo = local.get();
        OutputStream baos = new ByteBufferOutputStream(dst);
        try (Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, data);
        }
    }

    @Override
    public T fromBytes(ByteBuffer buffer) {
        Kryo kryo = local.get();
        InputStream inputStream = new ByteBufferInputStream(buffer);
        try (Input input = new Input(inputStream)) {
            return (T) kryo.readClassAndObject(input);
        }
    }
}
