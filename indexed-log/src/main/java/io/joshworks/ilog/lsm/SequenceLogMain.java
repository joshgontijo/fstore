package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.RecordBatch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SequenceLogMain {
    public static void main(String[] args) throws IOException {

        Threads.sleep(7000);

        long items = 20000000;

        BufferPool bufferPool = BufferPool.localCachePool(256, 1024, false);
        File root = TestUtils.testFolder();
        SequenceLog log = new SequenceLog(root, 1024, Size.MB.ofInt(500), 2, FlushMode.ON_ROLL, bufferPool);
        byte[] uuid = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        var bb = ByteBuffer.wrap(uuid);
        for (int i = 0; i < items; i++) {
            log.append(bb);
            bb.clear();
            if (i % 1000000 == 0) {
                System.out.println("WRITTEN: " + i);
            }
        }

        var buffer = Buffers.allocate(1024, false);
        var kb = Buffers.allocate(Long.BYTES, false);
        for (long i = 0; i < items; ) {
            buffer.clear();
            log.get(i, buffer);
            buffer.flip();

            if (!buffer.hasRemaining()) {
                System.err.println("No data for " + i);
            }

            while (RecordBatch.hasNext(buffer)) {
                int size = Record2.validate(buffer);
                kb.clear();
                Record2.writeKey(buffer, kb);
                kb.flip();
                Buffers.offsetPosition(buffer, size);

//                String toString = record.toString(Serializers.LONG, Serializers.STRING);
                long l = kb.getLong();
//                System.out.println(toString);
                if (l != i) {
                    throw new RuntimeException("Not sequential");
                }
                if (l % 1000000 == 0) {
                    System.out.println("READ: " + i);
                }
                i++;
            }

        }
    }
}