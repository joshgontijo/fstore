package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.StreamBlockDeserializer;
import io.joshworks.es2.sstable.TestEvent;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreTest {

    private EventStore store;
    private File root;


    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = new EventStore(root.toPath(), Executors.newSingleThreadExecutor());
    }

    @After
    public void tearDown() {
        store.close();
    }

    @Test
    public void version() {
        String stream = "stream-1";
        TestEvent ev1 = TestEvent.create(stream, Event.NO_VERSION, "type-a", "data-1");

        store.append(ev1.serialize());
        assertEquals(0, store.version(StreamHasher.hash(stream)));
    }

    @Test
    public void read_version_too_high() {
        String stream = "stream-1";
        TestEvent ev1 = TestEvent.create(stream, Event.NO_VERSION, "type-a", "data-1");

        store.append(ev1.serialize());

        Sink.Memory sink = new Sink.Memory();
        int res = store.read(StreamHasher.hash(stream), 1, sink);

        assertEquals(Event.VERSION_TOO_HIGH, res);
        assertEquals(0, sink.data().length);
    }

    @Test
    public void read() {
        String stream = "stream-1";
        TestEvent ev1 = TestEvent.create(stream, Event.NO_VERSION, "type-a", "data-1");
        TestEvent ev2 = TestEvent.create(stream, Event.NO_VERSION, "type-a", "data-2");

        store.append(ev1.serialize());
        store.append(ev2.serialize());

        Sink.Memory sink = new Sink.Memory();
        int read = store.read(StreamHasher.hash(stream), 0, sink);
        assertTrue(read > 0);

        List<TestEvent> events = StreamBlockDeserializer.deserialize(sink.data());
        assertEquals(2, events.size());

        assertEquals(0, events.get(0).version);
        assertEquals(1, events.get(1).version);
    }

}