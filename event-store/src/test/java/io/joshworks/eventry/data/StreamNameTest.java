package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;
import org.junit.Test;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;
import static org.junit.Assert.*;

public class StreamNameTest {

    @Test
    public void testtoString() {
    }

    @Test
    public void no_version() {
        StreamName stream = StreamName.of("stream");
        assertEquals(NO_VERSION, stream.version);
    }

    @Test
    public void no_version_less_than_NO_VERSION_uses_NO_VERSION() {
        StreamName stream = StreamName.of("stream@-11111");
        assertEquals("stream", stream.name);
        assertEquals(NO_VERSION, stream.version);
    }

    @Test
    public void with_version() {
        StreamName stream = StreamName.of("stream@1");
        assertEquals("stream", stream.name);
        assertEquals(1, stream.version);
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiple_ampersands_throw_exception() {
        StreamName.of("stream@@1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiple_ampersands_throw_exception2() {
        StreamName.of("stream@1@");
    }

    @Test
    public void of2() {
    }

    @Test
    public void parse() {
    }

    @Test
    public void toString1() {
    }
}