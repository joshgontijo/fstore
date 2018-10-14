package io.joshworks.fstore.log;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class MMapBlockSegmentTest extends BlockSegmentTest {

    private final int bufferSize = (int) Size.KILOBYTE.toBytes(512);

    @Override
    Log<String> open(File file) {
        return new BlockSegment<>(
                new MMapStorage(file, Size.MEGABYTE.toBytes(10), Mode.READ_WRITE, bufferSize),
                Serializers.STRING, new DataStream(START), "magic", Type.LOG_HEAD,
                VLenBlock.factory(), new SnappyCodec(), blockSize);
    }

}
