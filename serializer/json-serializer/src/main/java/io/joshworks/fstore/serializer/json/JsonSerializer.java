package io.joshworks.fstore.serializer.json;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.StringSerializer;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
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

    public static Map<String, Object> fromString(String json) {
        return gson.fromJson(json, new TypeToken<Map<String, Object>>() {
        }.getType());
    }

    public static Map<String, Object> toMap(Object pojo) {
        if (pojo == null) {
            throw new IllegalArgumentException("Cannot convert null object");
        }
        if (!canBeSerialized(pojo)) {
            throw new IllegalArgumentException("Cannot convert " + pojo + " into json, not a valid json object");
        }
        return fromString(gson.toJson(pojo));
    }

    public static String toJson(Object data) {
        return gson.toJson(data);
    }

    public static byte[] toJsonBytes(Map<String, Object> data) {
        return toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer toBytes(T event) {
        return stringSerializer.toBytes(gson.toJson(event));
    }

    @Override
    public void writeTo(T data, ByteBuffer dest) {
        stringSerializer.writeTo(gson.toJson(data), dest);
    }

    @Override
    public T fromBytes(ByteBuffer data) {
        return gson.fromJson(stringSerializer.fromBytes(data), type);
    }

    private static boolean canBeSerialized(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof String) {
            return false;
        }
        if (o instanceof Number) {
            return false;
        }
        if (o instanceof Boolean) {
            return false;
        }
        if (o instanceof Collection) {
            return false;
        }
        return true;
    }

}
