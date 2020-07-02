package io.joshworks.ilog;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.Lsm;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class AppTest {
    public static void main(String[] args) {

        Threads.sleep(7000);
        int items = 1000000;

        final Lsm lsm = Lsm.create(TestUtils.testFolder(), KeyComparator.LONG)
                .memTable(1000000, Size.MB.ofInt(50), true)
                .codec(new SnappyCodec())
                .compactionThreads(1)
                .compactionThreshold(5)
                .open();


        long s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            ByteBuffer record = create(i, "value-" + i);
            lsm.append(record);
            if (i % 1000000 == 0) {
                System.out.println("-> " + i + ": " + (System.currentTimeMillis() - s));
                s = System.currentTimeMillis();
            }
        }

        ByteBuffer recBuffer = ByteBuffer.allocate(4096);
        ByteBuffer keyBuff = ByteBuffer.allocate(Long.BYTES);

        for (int i = 0; i < items; i++) {
            recBuffer.clear();
            keyBuff.clear().putLong(i).flip();
            lsm.get(keyBuff, recBuffer);

            recBuffer.flip();

            int koffset = Record.KEY.offset(recBuffer);
            int voffset = Record.VALUE.offset(recBuffer);
            int vlen = Record.VALUE.len(recBuffer);

            long key = recBuffer.getLong(koffset);

            byte[] b = new byte[vlen];
            recBuffer.get(voffset, b, 0, b.length);
            String value = new String(b, StandardCharsets.UTF_8);

            System.out.println("KEY: " + key + " -> " + value);

        }


    }

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


}
