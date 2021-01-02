package io.joshworks.es2;

import io.joshworks.es2.sstable.TestEvent;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class EventStoreTest {

    private EventStore store;
    private File root;


    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = new EventStore(root.toPath());
    }

    @Test
    public void version() {
        String stream = "stream-1";
        TestEvent ev1 = TestEvent.create(stream, Event.NO_VERSION, 0, "type-a", "data-1");

        store.append(ev1.serialize());
        assertEquals(0, store.version(StreamHasher.hash(stream)));
    }

    @Test
    public void read() {
    }

    @Test
    public void append() {
    }
}