package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;
import org.junit.Test;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;
import static io.joshworks.eventry.index.Range.START_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamNameTest {

    @Test
    public void toStringFormat() {
        assertEquals("stream@0", StreamName.of("stream").toString());
    }

    @Test
    public void toStringFormatFromCreate() {
        assertEquals("stream@123", StreamName.create("stream", 123).toString());
    }

    @Test
    public void no_version() {
        StreamName stream = StreamName.of("stream");
        assertEquals(START_VERSION, stream.version());
    }

    @Test
    public void name_is_correct() {
        assertEquals("stream", StreamName.of("stream@123").name());
    }

    @Test
    public void version_is_correct() {
        assertEquals(123, StreamName.of("stream@123").version());
    }

    @Test
    public void isAll() {
        assertTrue(StreamName.of(SystemStreams.ALL).isAll());
    }

    @Test
    public void isSystemStream() {
        assertTrue(StreamName.of(SystemStreams.STREAMS).isSystemStream());
    }

    @Test
    public void no_version_less_than_NO_VERSION_uses_NO_VERSION() {
        StreamName stream = StreamName.of("stream@-11111");
        assertEquals("stream", stream.name());
        assertEquals(NO_VERSION, stream.version());
    }

    @Test
    public void with_version() {
        StreamName stream = StreamName.of("stream@1");
        assertEquals("stream", stream.name());
        assertEquals(1, stream.version());
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiple_ampersands_throw_exception() {
        StreamName.of("stream@@1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiple_ampersands_throw_exception2() {
        StreamName.of("stream@1@");
    }


}