package io.joshworks.es;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static io.joshworks.fstore.core.io.buffers.Buffers.wrap;

public class EventStoreTest {

    private EventStore store;
    private File root;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = open();
    }

    private EventStore open() {
        return new EventStore(root, Size.MB.ofInt(1), 100);
    }

    @Test
    public void append_get() {
        long stream = 123;

        store.append(stream, -1, wrap("abc"));
        Event event = store.get(stream, 0);


    }


}