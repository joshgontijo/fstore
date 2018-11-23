package io.joshworks.eventry.it;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.JsonEvent;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class GithubIT {

    private File directory;
    private EventStore store;
    private static final String SOURCE = "github.json";

    @Before
    public void setUp() {
        directory = FileUtils.testFolder();
        store = EventStore.open(directory);
    }

    @After
    public void tearDown() {
        store.close();
        FileUtils.tryDelete(new File(directory, "index"));
        FileUtils.tryDelete(new File(directory, "projections"));
        FileUtils.tryDelete(directory);
    }

    //TODO parse json and use actual event type
    @Test
    public void github() throws IOException {
        String stream = "github";
        String evType = "evType";

        try (BufferedReader reader = openReader(SOURCE)) {
            String line;
            while((line = reader.readLine()) != null) {
                EventRecord record = EventRecord.create(stream, evType, line);
                store.append(record);
            }
        }

        try(LogIterator<EventRecord> iterator = store.eventLog.iterator(Direction.FORWARD)) {
            while(iterator.hasNext()) {
                EventRecord next = iterator.next();
                JsonEvent from = JsonEvent.from(next);
                System.out.println(from);
            }
        }
    }
    

    private static BufferedReader openReader(String file) {
        return new BufferedReader(new InputStreamReader(GithubIT.class.getClassLoader().getResourceAsStream(file)));
    }
}
