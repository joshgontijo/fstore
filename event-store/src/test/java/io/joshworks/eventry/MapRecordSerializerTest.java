package io.joshworks.eventry;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MapRecordSerializerTest {

    private final MapRecordSerializer serializer = new MapRecordSerializer();

    @Test
    public void serialize() {

        var map = new HashMap<String, Object>();
        map.put("string", "val1");
        map.put("int", 1);
        map.put("long", 123L);
        map.put("null", null);
        map.put("float", 456f);
        map.put("double", 789d);
        map.put("boolean", true);
        map.put("map", Map.of("a", 1, "b", 2, "c", 3));
        map.put("array", List.of(1, 2, 3, 4));

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertEquals(map.get("string"), result.get("string"));
        assertEquals(map.get("int"), result.get("int"));
        assertEquals(map.get("long"), result.get("long"));
        assertNull(result.get("null"));
        assertEquals(map.get("null"), result.get("null"));
        assertEquals(map.get("float"), result.get("float"));
        assertEquals(map.get("double"), result.get("double"));
        assertEquals(map.get("boolean"), result.get("boolean"));

        Map<String, Object> node = (Map<String, Object>) map.get("map");
        Map<String, Object> nodeResult = (Map<String, Object>) result.get("map");
        assertEquals(node.get("a"), nodeResult.get("a"));
        assertEquals(node.get("b"), nodeResult.get("b"));
        assertEquals(node.get("c"), nodeResult.get("c"));

        Collection col = (Collection) map.get("array");
        Collection colResult = (Collection) result.get("array");
        assertThat(col, is(colResult));
    }

    @Test
    public void serialize_with_int() {
        var map = new HashMap<String, Object>();
        map.put("string", "abcd");

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("string"));
        assertEquals(map.get("string"), result.get("string"));
    }

    @Test
    public void serialize_with_boolean() {
        var map = new HashMap<String, Object>();
        map.put("boolean", true);

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("boolean"));
        assertTrue((Boolean) map.get("boolean"));
    }

    @Test
    public void serialize_with_long() {
        var map = new HashMap<String, Object>();
        map.put("long", 1234L);

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("long"));
        assertEquals(map.get("long"), result.get("long"));
    }

    @Test
    public void serialize_with_double() {
        var map = new HashMap<String, Object>();
        map.put("double", 456d);

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("double"));
        assertEquals(map.get("double"), result.get("double"));
    }

    @Test
    public void serialize_with_float() {
        var map = new HashMap<String, Object>();
        map.put("float", 765F);

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("float"));
        assertEquals(map.get("float"), result.get("float"));
    }

    @Test
    public void serialize_with_string() {

        var map = new HashMap<String, Object>();
        map.put("int", 1);

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("int"));
        assertEquals(map.get("int"), result.get("int"));
    }

    @Test
    public void serialize_with_null() {

        var map = new HashMap<String, Object>();
        map.put("null", null);

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("null"));
        assertNull(result.get("null"));
    }

    @Test
    public void serialize_with_array() {

        var array = List.of(1, 2, 3, 4);
        var map = new HashMap<String, Object>();
        map.put("array", array);

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("array"));

        Collection col = (Collection) map.get("array");
        Collection colResult = (Collection) result.get("array");
        assertThat(col, is(colResult));
    }

    @Test
    public void serialize_node() {
        var node = Map.of("a", 1, "b", 2, "c", 3);

        var map = new HashMap<String, Object>();
        map.put("map", node);

        ByteBuffer byteBuffer = serializer.toBytes(map);
        Map<String, Object> result = serializer.fromBytes(byteBuffer);

        assertTrue(result.containsKey("map"));

        Map<String, Object> nodeResult = (Map<String, Object>) result.get("map");
        assertEquals(node.get("a"), nodeResult.get("a"));
        assertEquals(node.get("b"), nodeResult.get("b"));
        assertEquals(node.get("c"), nodeResult.get("c"));
    }

}