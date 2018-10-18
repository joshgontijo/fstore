package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.seda.TimeWatch;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.Test;

import java.util.UUID;

public class CacheTest {

    @Test
    public void name() {

        IEventStore store = EventStore.open(FileUtils.testFolder());

        TimeWatch watch = TimeWatch.start();
        int items = 5000000;
        for (int i = 0; i < items; i++) {
            store.append(EventRecord.create("stream-a", "type-a", UUID.randomUUID().toString()));
        }
        System.out.println("WRITE: " + watch.time());

        watch.reset();
        for (int i = 0; i < items; i++) {
            EventRecord found = store.get("stream-a", i);
        }
        System.out.println("NOT CACHED: " + watch.time());

        watch.reset();
        for (int i = 0; i < items; i++) {
            EventRecord found = store.get("stream-a", i);
        }
        System.out.println("CACHED: " + watch.time());

    }
}
