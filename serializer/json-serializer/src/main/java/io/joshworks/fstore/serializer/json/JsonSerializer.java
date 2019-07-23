package io.joshworks.fstore.serializer.json;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.StringSerializer;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;


public class JsonSerializer<T> implements Serializer<T> {

    private static final Gson gson = new Gson();
    private static final StringSerializer stringSerializer = new StringSerializer();

    private final Type type;

    private JsonSerializer(Type type) {
        this.type = type;
    }

    public static <T> JsonSerializer<T> of(Class<T> type) {
        return new JsonSerializer<>(type);
    }

    public static <T> JsonSerializer<T> of(Type type) {
        return new JsonSerializer<>(type);
    }

    public static Map<String, Object> toMap(String json) {
        return gson.fromJson(json, new TypeToken<Map<String, Object>>() {
        }.getType());
    }

    public static String toJson(Object data) {
        return gson.toJson(data);
    }

    public static byte[] toBytes(Object data) {
        return toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(Map<String, Object> data) {
        return toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    public static <T> T fromBytes(byte[] data, Type type) {
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), type);
    }

    @Override
    public void writeTo(T data, ByteBuffer dst) {
        stringSerializer.writeTo(gson.toJson(data), dst);
    }

    @Override
    public T fromBytes(ByteBuffer data) {
        return gson.fromJson(stringSerializer.fromBytes(data), type);
    }
}
