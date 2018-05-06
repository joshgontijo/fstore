package io.joshworks.fstore.es.index.midpoint;

import io.joshworks.fstore.es.index.IndexEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class MidpointsTest {


    private File location;
    private Midpoints midpoints;

    @Before
    public void setUp() throws Exception {
        location = Files.createTempFile(null, null).toFile();
        midpoints = new Midpoints(location, "test");
    }

    @After
    public void tearDown() throws Exception {
        Files.deleteIfExists(location.toPath());
    }

    @Test
    public void midpoint_return_the_previous_index_for_non_exact_match() {

        //given
        midpoints.add(midpoint(1, 1));
        midpoints.add(midpoint(10, 1));
        midpoints.add(midpoint(100, 1));

        IndexEntry key = IndexEntry.of(2, 1, 0);

        //when
        int idx = midpoints.getMidpointIdx(key);

        //then
        assertEquals(0, idx);
    }

    @Test
    public void midpoint_return_the_previous_index_for_exact_match() {

        //given
        midpoints.add(midpoint(1, 1));
        midpoints.add(midpoint(10, 1));
        midpoints.add(midpoint(100, 1));

        IndexEntry key = IndexEntry.of(1, 1, 0);

        //when
        int idx = midpoints.getMidpointIdx(key);

        //then
        assertEquals(0, idx);
    }


    private Midpoint midpoint(long stream, int version) {
        return midpoint(stream, version, 0);
    }

    private Midpoint midpoint(long stream, int version, long pos) {
        return new Midpoint(IndexEntry.of(stream, version, pos), 0);
    }

    @Test
    public void add() {
    }

    @Test
    public void write() {
    }

    @Test
    public void getMidpointIdx() {
    }

    @Test
    public void getMidpointFor() {
    }

    @Test
    public void delete() {
    }

    @Test
    public void inRange() {
    }

    @Test
    public void size() {
    }

    @Test
    public void first() {
    }

    @Test
    public void last() {
    }
}