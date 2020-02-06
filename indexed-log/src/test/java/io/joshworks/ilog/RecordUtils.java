//package io.joshworks.ilog;
//
//import io.joshworks.fstore.core.Serializer;
//import io.joshworks.fstore.core.io.buffers.Buffers;
//import io.joshworks.fstore.serializer.Serializers;
//
//import java.nio.ByteBuffer;
//
//public class RecordUtils {
//
//    public static <T> T readKey(ByteBuffer record, Serializer<T> serializer) {
//        return Record2.readKey(record, serializer);
//    }
//
//    public static <T> T readValue(ByteBuffer record, Serializer<T> serializer) {
//        return Record2.readValue(record, serializer);
//    }
//
//    public static ByteBuffer copy(ByteBuffer record) {
//        if (!RecordBatch.hasNext(record)) {
//            throw new IllegalArgumentException("No record");
//        }
//        int rSize = Record2.sizeOf(record);
//        var dst = ByteBuffer.allocate(rSize);
//        Buffers.fill(record, dst);
//        dst.flip();
//        Record2.validate(dst);
//        return dst;
//    }
//
//    public static ByteBuffer create(long key, String val) {
//        return create(key, Serializers.LONG, val, Serializers.STRING);
//    }
//
//    public static <K, V> ByteBuffer create(K key, Serializer<K> ks, V value, Serializer<V> vs) {
//        var kb = Buffers.allocate(128, false);
//        var vb = Buffers.allocate(64, false);
//        var dst = Buffers.allocate(256, false);
//
//        ks.writeTo(key, kb);
//        kb.flip();
//
//        vs.writeTo(value, vb);
//        vb.flip();
//
//        Record2.create(kb, vb, dst);
//        dst.flip();
//        return dst;
//    }
//
//    public static <K, V> String toString(ByteBuffer buffer, Serializer<K> ks, Serializer<V> vs) {
//        int rsize = Record2.validate(buffer);
//        ByteBuffer copy = Buffers.allocate(rsize, false);
//        Buffers.copy(buffer, buffer.position(), rsize, copy);
//        copy.flip();
//        K k = Record2.readKey(copy, ks);
//        V v = Record2.readValue(copy, vs);
//
//        return "Record{" +
//                " recordLength=" + Record2.sizeOf(buffer) +
//                ", checksum=" + Record2.checksum(buffer) +
//                ", keyLength=" + Record2.keySize(buffer) +
//                ", dataLength=" + Record2.valueSize(buffer) +
//                ", timestamp=" + Record2.timestamp(buffer) +
//                ", key=" + k +
//                ", value=" + v +
//                '}';
//    }
//
//}
