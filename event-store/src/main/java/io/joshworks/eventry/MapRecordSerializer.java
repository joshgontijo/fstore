package io.joshworks.eventry;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapRecordSerializer implements Serializer<Map<String, Object>> {

    private static final int TYPE_LEN = 1;

    private static final int NULL = 0;
    private static final int STRING = 1;
    private static final int INTEGER = 2;
    private static final int DOUBLE = 3;
    private static final int FLOAT = 4;
    private static final int LONG = 5;
    private static final int BOOLEAN = 6;
    private static final int OBJECT = 7;
    private static final int ARRAY = 8;


    @Override
    public ByteBuffer toBytes(Map<String, Object> data) {
        List<ByteBuffer> buffers = new ArrayList<>();
        int totalSize = 0;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();

            ByteBuffer keyData = serializeVString(key);
            buffers.add(keyData);

            ByteBuffer valData = serializeValue(val);
            buffers.add(valData);

            totalSize += keyData.limit() + valData.limit();
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (ByteBuffer byteBuffer : buffers) {
            buffer.put(byteBuffer);
        }
        return buffer.flip();
    }

    @Override
    public void writeTo(Map<String, Object> data, ByteBuffer dest) {
        ByteBuffer byteBuffer = toBytes(data);
        dest.put(byteBuffer);
    }

    @Override
    public Map<String, Object> fromBytes(ByteBuffer buffer) {
        Map<String, Object> map = new HashMap<>();
        while (buffer.hasRemaining()) {
            String key = deserializeVString(buffer);
            Object value = deserializeValue(buffer);
            map.put(key, value);
        }
        return map;
    }

    private Object deserializeValue(ByteBuffer buffer) {
        int type = buffer.get();
        switch (type) {
            case NULL:
                return null;
            case INTEGER:
                return buffer.getInt();
            case LONG:
                return buffer.getLong();
            case DOUBLE:
                return buffer.getDouble();
            case FLOAT:
                return buffer.getFloat();
            case BOOLEAN:
                return buffer.get() == 1;
            case STRING:
                return deserializeVString(buffer);
            case OBJECT:
                return deserializeNode(buffer);
            case ARRAY:
                return deserializeCollection(buffer);
            default:
                throw new IllegalStateException("Invalid type: " + type);
        }
    }

    private Object deserializeNode(ByteBuffer buffer) {
        int nodeSize = buffer.getInt();
        int limit = buffer.limit();
        buffer.limit(buffer.position() + nodeSize);
        Map<String, Object> node = fromBytes(buffer);
        buffer.limit(limit);
        return node;
    }

    //value format
    //TYPE[1 byte][VAL]
    private ByteBuffer serializeValue(Object obj) {
        if (obj == null) {
            return ByteBuffer.allocate(1).put((byte) NULL).flip();
        }
        Class<?> type = obj.getClass();

        if (String.class.equals(type)) {
            String val = (String) obj;
            ByteBuffer data = serializeVString(val);
            ByteBuffer bb = ByteBuffer.allocate(TYPE_LEN + data.limit());
            return bb.put((byte) STRING).put(data).flip();
        } else if (Double.class.equals(type) || Double.TYPE.equals(type)) {
            return ByteBuffer.allocate(TYPE_LEN + Double.BYTES).put((byte) DOUBLE).putDouble((double) obj).flip();
        } else if (Float.class.equals(type) || Float.TYPE.equals(type)) {
            return ByteBuffer.allocate(TYPE_LEN + Float.BYTES).put((byte) FLOAT).putFloat((float) obj).flip();
        } else if (Integer.class.equals(type) || Integer.TYPE.equals(type)) {
            return ByteBuffer.allocate(TYPE_LEN + Integer.BYTES).put((byte) INTEGER).putInt((int) obj).flip();
        } else if (Long.class.equals(type) || Long.TYPE.equals(type)) {
            return ByteBuffer.allocate(TYPE_LEN + Long.BYTES).put((byte) LONG).putLong((long) obj).flip();
        } else if (Boolean.class.equals(type) || Boolean.TYPE.equals(type)) {
            boolean b = (boolean) obj;
            return ByteBuffer.allocate(TYPE_LEN + Byte.BYTES).put((byte) BOOLEAN).put((byte) (b ? 1 : 0)).flip();
        }

        if (obj instanceof Map) {
            Map<String, Object> node = (Map<String, Object>) obj;
            ByteBuffer nodeData = toBytes(node);
            ByteBuffer buffer = ByteBuffer.allocate(TYPE_LEN + Integer.BYTES + nodeData.limit());
            return buffer.put((byte) OBJECT).putInt(nodeData.limit()).put(nodeData).flip();
        }

        //collection
        if (obj instanceof Collection) {
            return serializeCollection((Collection) obj);
        }

        throw new IllegalArgumentException("Invalid value type " + type);
    }

    private ByteBuffer serializeCollection(Collection collection) {
        List<ByteBuffer> arrayBuffers = new ArrayList<>();
        int totalSize = 0;
        for (Object item : collection) {
            ByteBuffer byteBuffer = serializeValue(item);
            arrayBuffers.add(byteBuffer);
            totalSize += byteBuffer.limit();
        }
        int size = collection.size();
        ByteBuffer colBuffer = ByteBuffer.allocate(TYPE_LEN + Integer.BYTES + totalSize);
        colBuffer.put((byte) ARRAY);
        colBuffer.putInt(size);
        for (ByteBuffer arrayBuffer : arrayBuffers) {
            colBuffer.put(arrayBuffer);
        }
        return colBuffer.flip();
    }

    private Collection deserializeCollection(ByteBuffer buffer) {
        int size = buffer.getInt();
        List<Object> items = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Object item = deserializeValue(buffer);
            items.add(item);
        }
        return items;
    }

    private ByteBuffer serializeVString(String data) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES + bytes.length);
        return bb.putInt(bytes.length).put(bytes).flip();
    }

    private String deserializeVString(ByteBuffer buffer) {
        int length = buffer.getInt(); //LEN_LEN
        if (!buffer.hasArray()) {
            byte[] data = new byte[length];
            buffer.get(data);
            return new String(data, StandardCharsets.UTF_8);
        }

        String value = new String(buffer.array(), buffer.position(), length, StandardCharsets.UTF_8);
        buffer.position(buffer.position() + length);
        return value;
    }

}
