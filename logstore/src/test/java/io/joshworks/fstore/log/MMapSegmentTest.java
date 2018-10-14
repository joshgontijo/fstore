package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class MMapSegmentTest extends DefaultSegmentTest {

    @Override
    Log<String> open(File file) {
        return new Segment<>(
                new MMapStorage(file, Size.MEGABYTE.toBytes(10), Mode.READ_WRITE, 4096),
                Serializers.STRING,
                new DataStream(Segment.START), "magic", Type.LOG_HEAD);
    }
}
