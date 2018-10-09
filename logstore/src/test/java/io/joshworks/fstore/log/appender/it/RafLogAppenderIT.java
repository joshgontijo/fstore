package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class RafLogAppenderIT extends LogAppenderIT {

    @Override
    protected LogAppender<String> appender(File testDirectory) {
        return LogAppender.builder(testDirectory, Serializers.STRING)
                .segmentSize(524288000)
//                .disableCompaction()
                .threadPerLevelCompaction()
                .open();
    }
}
