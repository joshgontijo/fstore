package io.joshworks.fstore.log.it;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class CachedRafLogAppenderIT extends LogAppenderIT {

    @Override
    protected LogAppender<String> appender(File testDirectory) {
        return LogAppender.builder(testDirectory, Serializers.STRING)
                .segmentSize(Size.MB.of(500))
                .storageMode(StorageMode.RAF_CACHED)
                .disableCompaction()
                .open();
    }

}
