package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {

        File folder = TestUtils.testFolder();

        Log log = new Log(folder, 4096, Size.MB.ofInt(10), FlushMode.ON_ROLL, IndexedSegment::new, LongIndex::new);

        ByteBuffer writeBuffer = Buffers.allocate(64, false);
        for (long i = 0; i < 25000000; i++) {
            Record record = Record.create(i, Serializers.LONG, "value-" + i, Serializers.VSTRING, writeBuffer);
            log.append(record);
            writeBuffer.clear();
            if (i % 100000 == 0) {
                System.out.println("-> " + i);
            }
        }

       Thread.sleep(30000000);

    }
}
