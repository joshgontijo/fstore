package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Ignore;

import java.io.File;

@Ignore
public class MmapSimpleLogAppenderIT extends LogAppenderIT {

    @Override
    protected LogAppender<String> appender(File testDirectory) {
//        return new SimpleLogAppender<>(LogAppender.builder(testDirectory, Serializers.STRING).mmap());
        return LogAppender.builder(testDirectory, Serializers.STRING)
                .mmap(83986080)
                .segmentSize(83886080)
                .open();
    }
}
