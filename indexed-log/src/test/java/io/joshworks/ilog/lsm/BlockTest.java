package io.joshworks.ilog.lsm;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.RecordUtils;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class BlockTest {

    @Test
    public void writeBlock() {
        int items = 10;
        SnappyCodec codec = new SnappyCodec();
        ByteBuffer blockRecords = Buffers.allocate(4096, false);
        ByteBuffer block = Buffers.allocate(4096, false);

        for (int i = 0; i < items; i++) {
            ByteBuffer record = RecordUtils.create(i, "value-" + i);
            blockRecords.put(record);
        }

        blockRecords.flip();
        int entries = Block.create(blockRecords, block, Long.BYTES, codec);

        assertEquals(items, entries);

        ByteBuffer dst = Buffers.allocate(4096, false);
        Block.decompress(block, dst, codec);


        assertEquals(blockRecords.flip(), dst.flip());
    }
}