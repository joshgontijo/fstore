//package io.joshworks.ilog.lsm;
//
//import io.joshworks.fstore.core.io.buffers.Buffers;
//import io.joshworks.fstore.serializer.Serializers;
//import io.joshworks.ilog.Record;
//import io.joshworks.ilog.record.RecordUtils;
//
//import java.nio.ByteBuffer;
//
//public class LsmRecordUtils {
//
//    static ByteBuffer add(long key, String value) {
//        return RecordUtils.create(key, value);
//    }
//
//    static ByteBuffer delete(long key) {
//        var kb = Buffers.allocate(Long.BYTES, false);
//        var dst = Buffers.allocate(256, false);
//
//        Serializers.LONG.writeTo(key, kb);
//        kb.flip();
//
//        Record.create(kb, ByteBuffer.allocate(0), dst, RecordFlags.DELETION_ATTR);
//        dst.flip();
//
//        return dst;
//    }
//
//
//}
