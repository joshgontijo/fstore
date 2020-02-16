package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RecordUtils {

    public static ByteBuffer create(long key, String val) {
        return create(key, Serializers.LONG, val, Serializers.STRING);
    }

    public static <K, V> ByteBuffer create(K key, Serializer<K> ks, V value, Serializer<V> vs) {
        var kb = Buffers.allocate(128, false);
        var vb = Buffers.allocate(64, false);
        var dst = Buffers.allocate(256, false);

        ks.writeTo(key, kb);
        kb.flip();

        vs.writeTo(value, vb);
        vb.flip();

        Record.create(kb, vb, dst);
        dst.flip();
        return dst;
    }

    public static long readKey(ByteBuffer record) {
        var dst = Buffers.allocate(Record.KEY_LEN.get(record), false);
        Record.KEY_LEN.copyTo(record, dst);
        dst.flip();
        return dst.getLong();
    }

    public static String readValue(ByteBuffer record) {
        var dst = Buffers.allocate(Record.VALUE_LEN.get(record), false);
        Record.VALUE.copyTo(record, dst);
        dst.flip();
        return StandardCharsets.UTF_8.decode(dst).toString();
    }


}
