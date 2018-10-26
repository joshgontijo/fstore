package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Test;

import java.io.File;

public class RafSegmentTest extends SegmentTest {

    @Override
    Log<String> open(File file) {
        return new Segment<>(
                StorageProvider.of(StorageMode.RAF).create(file, Size.MB.of(10)),
                Serializers.STRING,
                new DataStream(new SingleBufferThreadCachedPool(false)),
                "magic",
                Type.LOG_HEAD);
    }

    @Test(expected = IllegalArgumentException.class)
    public void inserting_record_bigger_than_MAX_RECORD_SIZE_throws_exception() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < DataStream.MAX_ENTRY_SIZE + 1; i++) {
            sb.append("a");
        }
        String data = sb.toString();
        segment.append(data);
        segment.flush();
    }
}
