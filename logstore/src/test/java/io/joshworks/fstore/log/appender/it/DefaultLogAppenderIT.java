package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class DefaultLogAppenderIT extends LogAppenderIT {

    @Override
    protected LogAppender<String> appender(File testDirectory) {
        return LogAppender.builder(testDirectory, Serializers.STRING)
                .segmentSize(83886080)
                .threadPerLevelCompaction()
                .open();
    }
}
