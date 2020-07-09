package io.joshworks.ilog;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.Lsm;
import io.joshworks.ilog.record.BufferRecords;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.nio.ByteBuffer;

public class AppTest {

    private static final int memTableSize = 1000;

    private static RecordPool pool = RecordPool.create(RowKey.LONG).batchSize(memTableSize + 1).build();

    public static void main(String[] args) {


        final Lsm lsm = Lsm.create(TestUtils.testFolder(), RowKey.LONG)
                .memTable(memTableSize, Size.MB.ofInt(50), true)
                .codec(new SnappyCodec())
                .compactionThreads(1)
                .compactionThreshold(5)
                .open();

        BufferRecords records = pool.empty();
        for (int i = 0; i < memTableSize + 1; i++) {
            records.add(create(i, "value-" + i));
        }
        lsm.append(records);




        for (int i = 0; i < memTableSize + 1; i++) {
            Records found = lsm.get(ByteBuffer.allocate(Long.BYTES).putLong(i).flip());
            if (found == null) {
                throw new RuntimeException("Failed: " + i);
            }
        }
        System.out.println("Done");
        lsm.close();

    }

    public static Record2 create(long key, String val) {
        return create(key, RowKey.LONG, Serializers.LONG, val, Serializers.STRING);
    }

    public static <K, V> Record2 create(K key, RowKey rk, Serializer<K> ks, V value, Serializer<V> vs) {
        var kb = Buffers.allocate(128, false);
        var vb = Buffers.allocate(64, false);
        var dst = Buffers.allocate(256, false);

        ks.writeTo(key, kb);
        kb.flip();

        vs.writeTo(value, vb);
        vb.flip();

        return Record2.create(0, rk, kb, vb);
    }


}
