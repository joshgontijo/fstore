package io.joshworks.es.log;

import io.joshworks.es.Event;
import io.joshworks.es.EventHelper;
import io.joshworks.es.SegmentDirectory;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogSegmentTest {

    public static final long INITIAL_SIZE = Size.MB.of(1);
    private File testFile;
    private LogSegment segment;

    @Before
    public void setUp() {
        testFile = TestUtils.testFile(SegmentDirectory.segmentFileName(1, 3, "abc", 100));
        segment = LogSegment.create(testFile, INITIAL_SIZE);
    }

    @After
    public void tearDown() {
        segment.delete();
    }

    @Test
    public void segment_size_is_set_on_create() {
        assertEquals(INITIAL_SIZE, segment.size());
    }

    @Test
    public void append_read() {
        long pos = segment.append(create(0));
        assertSingle(pos, 0);
    }

    @Test
    public void iterator() {
        int items = 100;
        for (int i = 0; i < items; i++) {
            segment.append(create(i));
        }

        SegmentIterator it = segment.iterator();

        int found = 0;
        while (it.hasNext()) {
            ByteBuffer next = it.next();
            assertTrue(Event.isValid(next));
            assertEquals(found, Event.sequence(next));
            found++;
        }
        assertEquals(items, found);
    }

    @Test
    public void append_restore() {
        int items = 100;
        for (int i = 0; i < items; i++) {
            segment.append(create(i));
        }

        long pos = segment.writePosition();
        long restore = segment.restore(Event::isValid);
        assertEquals(items, restore);
        assertEquals(pos, segment.writePosition());
    }

    @Test
    public void append_close_restore() {
        int items = 100;
        for (int i = 0; i < items; i++) {
            segment.append(create(i));
        }

        long pos = segment.writePosition();

        segment.close();
        segment = LogSegment.open(testFile);

        long restore = segment.restore(Event::isValid);
        assertEquals(items, restore);
        assertEquals(pos, segment.writePosition());
    }

    private void assertSingle(long position, long sequence) {
        ByteBuffer dst = Buffers.allocate(4096, false);
        int read = segment.read(dst, position);
        assertTrue(read > 0);
        dst.flip();
        assertTrue(Event.isValid(dst));
        assertEquals(sequence, Event.sequence(dst));
    }

    private static ByteBuffer create(long seq) {
        return EventHelper.evOf(seq, "stream-1", 1, "test");
    }

}