package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.serializer.StringSerializer;

import java.io.File;

public class MMapLogAppenderTest extends LogAppenderTest {


    @Override
    protected LogAppender<String> appender(File testDirectory, int segmentSize) {
        return LogAppender.builder(testDirectory, new StringSerializer())
                .segmentSize(segmentSize)
                .disableCompaction()
                .storageMode(StorageMode.MMAP)
                .open();
    }
}