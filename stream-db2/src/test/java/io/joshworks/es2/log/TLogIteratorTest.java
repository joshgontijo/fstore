package io.joshworks.es2.log;

import io.joshworks.es2.Event;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TLogIteratorTest {


    private File folder;

    @Before
    public void init() {
        folder = TestUtils.testFolder();
    }

    @After
    public void tearDown() {
        TestUtils.deleteRecursively(folder);
    }

    @Test
    public void iteratorTest() {
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), a -> {
        });
        log.append(of(1));
        log.append(of(2));
        log.append(of(3));

        log.roll();
        log.append(of(4));
        log.append(of(5));

        var it = log.iterator();

        assertEquals(1, valueOf(it.next()));
        assertEquals(2, valueOf(it.next()));
        assertEquals(3, valueOf(it.next()));
        assertEquals(4, valueOf(it.next()));
        assertEquals(5, valueOf(it.next()));

        //idempotence
        for (int i = 0; i < 10; i++) {
            assertFalse(it.hasNext());
        }

        log.append(of(6));
        assertTrue(it.hasNext());
        assertEquals(6, valueOf(it.next()));
        assertFalse(it.hasNext());
    }

    @Test
    public void iterator_new_segment() {
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), a -> {
        });
        log.append(of(1));

        var it = log.iterator();

        assertEquals(1, valueOf(it.next()));
        assertFalse(it.hasNext());

        log.roll();
        assertFalse(it.hasNext());

        log.append(of(2));
        assertTrue(it.hasNext());
        assertEquals(2, valueOf(it.next()));
        assertFalse(it.hasNext());
    }


    private static ByteBuffer of(int val) {
        var data = ByteBuffer.allocate(Integer.BYTES).putInt(val).array();
        return Event.create(123, Event.NO_VERSION, "TEST_EVENT", data);
    }

    private static int valueOf(ByteBuffer event) {
        return ByteBuffer.wrap(Event.data(event)).getInt();
    }

}