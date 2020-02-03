package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.RecordUtils;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.joshworks.ilog.lsm.Block2.binarySearch;
import static io.joshworks.ilog.lsm.Block2.entries;
import static io.joshworks.ilog.lsm.Block2.entryOffset;

public class MemTableTest {


    private final KeyComparator comparator = KeyComparator.LONG;
    private Codec codec = Codec.noCompression();

    @Test
    public void name() {
        MemTable table = new MemTable(comparator, BufferPool.unpooled(4096, false));
        int items = 1000;
        for (int i = 0; i < items; i++) {
            table.add(create(i, "value-" + i));
        }

        var block = Buffers.allocate(4096, false);
        var valuesRegion = Buffers.allocate(4096, false);
        var dst = Buffers.allocate(4096 * 2, false);

        List<ByteBuffer> entries = new ArrayList<>();
        Consumer<ByteBuffer> writer = bb -> entries.add(copy(bb));

        table.writeTo(writer, -1, codec, block, valuesRegion, dst);


        int i = 0;
        for (var blockRecord : entries) {
            int blockEntries = entries(blockRecord);
            for (int j = 0; j < blockEntries; j++, i++) {
                ByteBuffer key = bufferOf(i);
                int idx = binarySearch(blockRecord, key, IndexFunctions.EQUALS, comparator);
                int offset = entryOffset(blockRecord, idx, comparator.keySize());
//                int len = entryLen(blockRecord, idx, comparator.keySize());

                System.out.println(idx);

                var decompressed = Buffers.allocate(4096, false);
                Block2.decompress(blockRecord, decompressed, codec);

                decompressed.flip();

                long timestamp = Block2.Record.timestamp(decompressed);
                int size = Block2.Record.valueSize(decompressed);
                System.out.println();

            }

        }

    }

    private static ByteBuffer create(long key, String value) {
        return RecordUtils.create(key, Serializers.LONG, value, Serializers.STRING);
    }

    private static ByteBuffer bufferOf(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).flip();
    }

    private static ByteBuffer copy(ByteBuffer src) {
        var dst = Buffers.allocate(src.remaining(), src.isDirect());
        Buffers.copy(src, dst);
        dst.flip();
        return dst;
    }

}