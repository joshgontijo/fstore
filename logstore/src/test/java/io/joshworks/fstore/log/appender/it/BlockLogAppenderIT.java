package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Ignore;

import java.io.File;

@Ignore
public class BlockLogAppenderIT extends LogAppenderIT {

    @Override
    protected LogAppender<String> appender(File testDirectory) {
        return LogAppender.builder(testDirectory, Serializers.STRING)
                .segmentSize(83886080)
                .openBlockAppender();
    }
}
