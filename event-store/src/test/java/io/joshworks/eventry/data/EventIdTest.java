package io.joshworks.eventry.data;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
import org.junit.Test;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EventIdTest {

    @Test
    public void toStringFormat() {
        assertEquals("stream", EventId.parse("stream").toString());
    }

    @Test
    public void toStringFormatFromCreate() {
        assertEquals("stream@123", EventId.of("stream", 123).toString());
    }

    @Test
    public void no_version() {
        EventId stream = EventId.parse("stream");
        assertEquals(NO_VERSION, stream.version());
        assertFalse(stream.hasVersion());
    }

    @Test
    public void name_is_correct() {
        assertEquals("stream", EventId.parse("stream@123").name());
    }

    @Test
    public void version_is_correct() {
        assertEquals(123, EventId.parse("stream@123").version());
    }

    @Test
    public void isAll() {
        assertTrue(EventId.parse(SystemStreams.ALL).isAll());
    }

    @Test
    public void isSystemStream() {
        assertTrue(EventId.parse(SystemStreams.STREAMS).isSystemStream());
    }

    @Test
    public void no_version_less_than_NO_VERSION_uses_NO_VERSION() {
        EventId stream = EventId.parse("stream@-11111");
        assertEquals("stream", stream.name());
        assertEquals(NO_VERSION, stream.version());
    }

    @Test
    public void with_version() {
        EventId stream = EventId.parse("stream@1");
        assertEquals("stream", stream.name());
        assertEquals(1, stream.version());
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiple_ampersands_throw_exception() {
        EventId.parse("stream@@1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiple_ampersands_throw_exception2() {
        EventId.parse("stream@1@");
    }


}