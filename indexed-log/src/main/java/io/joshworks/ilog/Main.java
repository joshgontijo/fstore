package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Main {

    public static void main(String[] args) throws IOException {

        Log log = new Log(TestUtils.testFolder(), 4096, Size.MB.ofInt(10), FlushMode.MANUAL, LongIndex::new);

        ByteBuffer writeBuffer = Buffers.allocate(64, false);
        for (long i = 0; i < 10000000; i++) {
            Record record = Record.create(i, Serializers.LONG, "value-" + i, Serializers.VSTRING, writeBuffer);
            log.append(record);
            writeBuffer.clear();
            if (i % 100000 == 0) {
                System.out.println("-> " + i);
            }
        }

        log.close();

    }
}
