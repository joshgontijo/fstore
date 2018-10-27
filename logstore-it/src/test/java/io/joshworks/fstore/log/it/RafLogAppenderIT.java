package io.joshworks.fstore.log.it;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class RafLogAppenderIT extends LogAppenderIT {

    @Override
    protected LogAppender<String> appender(File testDirectory) {
        return LogAppender.builder(testDirectory, Serializers.STRING)
                .segmentSize(Size.MB.of(500))
                .storageMode(StorageMode.RAF)
                .disableCompaction()
                .open();
    }



    //Not RAF specific, added to this class because required specific config, should be moved to a 'CompactionIT' or something
    @Test
    public void compaction_maintains_record_order() throws InterruptedException {
        File testDir = FileUtils.testFolder();
        LogAppender<String> appender = LogAppender.builder(testDir, Serializers.STRING).segmentSize(Size.MB.of(10)).compactionThreshold(2).open();

        int items = 20000000;
        for (int i = 0; i < items; i++) {
            appender.append(String.valueOf(i));
        }

        System.out.println("Waiting for compaction");
        Thread.sleep(120000);

        LogIterator<String> iterator = appender.iterator(Direction.FORWARD);

        int idx = 0;
        while(iterator.hasNext()) {
            String found = iterator.next();
            assertEquals(String.valueOf(idx++), found);
        }

    }
}
