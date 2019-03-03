package io.joshworks.eventry.index;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RangeTest {

    @Test
    public void range_returns_same_stream_for_start_and_end() {
        Range range = Range.of(1, 0);
        assertEquals(1, range.start().stream);
        assertEquals(1, range.end().stream);
    }

    @Test
    public void range_correct_version_range_for_single_stream() {
        Range range = Range.of(1, 0, 10);
        assertEquals(0, range.start().version);
        assertEquals(10, range.end().version);
    }

    @Test
    public void start() {
    }

    @Test
    public void end() {
    }
}