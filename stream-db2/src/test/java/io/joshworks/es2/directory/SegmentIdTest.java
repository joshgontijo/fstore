package io.joshworks.es2.directory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SegmentIdTest {


    @Test
    public void testCompare() {

        assertEquals(1, SegmentId.of(0, 1).compareTo(SegmentId.of(0, 2)));
        assertEquals(0, SegmentId.of(0, 2).compareTo(SegmentId.of(0, 2)));
        assertEquals(-1, SegmentId.of(0, 2).compareTo(SegmentId.of(0, 1)));


        assertEquals(-1, SegmentId.of(1, 0).compareTo(SegmentId.of(0, 0)));

    }
}