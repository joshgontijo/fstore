package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RecordUtils {

    public static final Serializer<Long> KS = Serializers.LONG;
    public static final Serializer<String> VS = Serializers.STRING;

    public static ByteBuffer recordBuffer(long key, String val) {
        Record record = create(key, val);
        ByteBuffer buffer = Buffers.allocate(record.recordSize(), false);
        record.copyTo(buffer);
        return buffer.flip();
    }

    public static Record create(long key, String val) {
        return create(key, KS, val, VS);
    }

    public static Records createN(long startKey, int count, RecordPool pool) {
        Records records = pool.empty();
        for (int i = 0; i < count; i++) {
            long key = startKey + i;
            Record rec = create(key, KS, "value-" + key, VS);
            if (!records.add(rec)) {
                throw new IllegalArgumentException("Records full");
            }
        }
        return records;
    }

    public static <K, V> Record create(K key, Serializer<K> ks, V value, Serializer<V> vs) {
        var kb = Buffers.allocate(128, false);
        var vb = Buffers.allocate(64, false);

        ks.writeTo(key, kb);
        kb.flip();

        vs.writeTo(value, vb);
        vb.flip();

        return Record.create(kb, vb);
    }

    public static long longKey(Record record) {
        short ks = record.keyLen();
        if (ks != Long.BYTES) {
            throw new RuntimeException("Invalid key length");
        }
        var dst = Buffers.allocate(ks, false);
        record.copyKey(dst);
        dst.flip();
        return dst.getLong();
    }

    public static String stringValue(Record record) {
        var dst = Buffers.allocate(record.valueSize(), false);
        record.copyValue(dst);
        dst.flip();
        return StandardCharsets.UTF_8.decode(dst).toString();
    }

    public static String toString(Record rec) {
        return "RECORD_LEN: " + rec.recordSize() + ", " +
                "CHECKSUM: " + rec.checksum() + ", " +
                "TIMESTAMP: " + rec.timestamp() + ", " +
                "SEQUENCE: " + rec.sequence() + ", " +
                "ATTRIBUTES: " + rec.attributes() + ", " +
                "KEY_LEN: " + rec.keyLen() + ", " +
                "KEY: " + longKey(rec) + ", " +
                "VALUE_LEN: " + rec.valueSize() + ", " +
                "VALUE: " + stringValue(rec);
    }

}
