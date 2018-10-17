package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class MMapLogAppenderIT extends LogAppenderIT {

    @Override
    protected LogAppender<String> appender(File testDirectory) {
        return LogAppender.builder(testDirectory, Serializers.STRING)
                .logSize((int) Size.MEGABYTE.toBytes(500))
                .threadPerLevelCompaction()
                .disableCompaction()
                .mmap()
                .open();
    }
}
