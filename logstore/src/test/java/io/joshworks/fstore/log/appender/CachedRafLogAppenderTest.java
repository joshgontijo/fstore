package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.serializer.StringSerializer;

import java.io.File;

public class CachedRafLogAppenderTest extends LogAppenderTest {

    private static final long CACHE_SIZE = Size.MEGABYTE.toBytes(200);

    @Override
    protected LogAppender<String> appender(File testDirectory, int segmentSize) {
        return LogAppender.builder(testDirectory, new StringSerializer())
                .segmentSize(segmentSize)
                .enableCaching(CACHE_SIZE)
                .disableCompaction()
                .open();
    }
}