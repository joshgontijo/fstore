//package io.joshworks.ilog.util;
//
//import java.nio.ByteBuffer;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.function.IntSupplier;
//
//import static java.util.Objects.requireNonNull;
//
//public class Format {
//
//    private final Map<String, Field> mapping = new HashMap<>();
//    public final int size;
//    public final String name;
//
//    private Format(String name, Map<String, Field> mapping) {
//        this.mapping.putAll(mapping);
//        this.name = name;
//        this.size = this.mapping.values().stream().mapToInt(f -> f.len).sum();
//    }
//
//    public static Builder builder(String name) {
//        return new Builder(name);
//    }
//
//    public int readInt(String name, ByteBuffer buffer) {
//        Field field = mapping.get(name);
//        return buffer.getInt(field.offset);
//    }
//
//    public int readLong(String name) {
//
//    }
//
//
//    public static class Builder {
//
//        private final Map<String, Field> map = new HashMap<>();
//        private String name;
//        private int offset;
//
//        private Builder(String name) {
//            this.name = name;
//        }
//
//        public Builder add(String name, int len) {
//            map.put(requireNonNull(name), new Field(offset, len));
//            this.offset += len;
//            return this;
//        }
//
//        public Builder add(String name, IntSupplier lenSupplier) {
//            map.put(requireNonNull(name), new Field(offset, len));
//            this.offset += len;
//            return this;
//        }
//
//        public Format build() {
//            return new Format(name, map);
//        }
//
//    }
//
//
//    private static class Field {
//        private final int offset;
//        private final int len;
//
//        private Field(int offset, int len) {
//            this.offset = offset;
//            this.len = len;
//        }
//    }
//
//}
