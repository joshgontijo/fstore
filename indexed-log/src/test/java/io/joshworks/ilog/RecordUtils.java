package io.joshworks.ilog;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.record.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RecordUtils {

    public static final Serializer<Long> KS = Serializers.LONG;
    public static final Serializer<String> VS = Serializers.STRING;


    public static ByteBuffer create(long key, String val) {
        var dst = Buffers.allocate(4096, false);
        int recSize = create(key, KS, val, VS, dst);
        dst.flip();
        return dst;
    }

    public static ByteBuffer createN(long startKey) {
        var dst = Buffers.allocate(4096, false);

        int recSize = 0;
        do {
            recSize = create(startKey, "value-" + startKey);
            startKey++;
        } while (recSize > 0);

        dst.flip();
        return dst;
    }

    public static <K, V> int create(K key, Serializer<K> ks, V value, Serializer<V> vs, ByteBuffer dst) {
        var kb = Buffers.allocate(128, false);
        var vb = Buffers.allocate(128, false);

        ks.writeTo(key, kb);
        kb.flip();

        if (value != null) {
            vs.writeTo(value, vb);
        }
        vb.flip();

        if (Record.computeRecordSize(kb.remaining(), vb.remaining()) < dst.remaining()) {
            return 0;
        }

        return Record.create(dst, kb, vb);
    }

    public static long longKey(ByteBuffer record) {
        short ks = Record.keyLen(record);
        if (ks != Long.BYTES) {
            throw new RuntimeException("Invalid key length");
        }
        var dst = Buffers.allocate(ks, false);
        Record.copyKey(record, dst);
        dst.flip();
        return dst.getLong();
    }

    public static String stringValue(ByteBuffer record) {
        var dst = Buffers.allocate(Record.valueLen(record), false);
        Record.copyValue(record, dst);
        dst.flip();
        return StandardCharsets.UTF_8.decode(dst).toString();
    }

    public static String toString(ByteBuffer rec) {
        if (rec == null) {
            return "null";
        }
        return "RECORD_LEN: " + Record.size(rec) + ", " +
                "CHECKSUM: " + Record.checksum(rec) + ", " +
                "TIMESTAMP: " + Record.timestamp(rec) + ", " +
                "SEQUENCE: " + Record.sequence(rec) + ", " +
                "ATTRIBUTES: " + Record.attributes(rec) + ", " +
                "KEY_LEN: " + Record.keyLen(rec) + ", " +
                "KEY: " + longKey(rec) + ", " +
                "VALUE_LEN: " + Record.valueLen(rec) + ", " +
                "VALUE: " + stringValue(rec);
    }

}
