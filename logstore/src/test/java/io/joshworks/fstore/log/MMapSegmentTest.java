package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class MMapSegmentTest extends RafSegmentTest {

    @Override
    Log<String> open(File file) {
        return new Segment<>(
                StorageProvider.mmap().create(file, Size.MB.of(10)),
                Serializers.STRING,
                new DataStream(new SingleBufferThreadCachedPool(false)), "magic", Type.LOG_HEAD);
    }
}
