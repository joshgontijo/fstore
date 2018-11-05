package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public abstract class BlockSegmentTest {

    protected BlockSegment<String> segment;
    private File testFile;

    abstract BlockSegment<String> open(File file);

    @Before
    public void setUp() {
        testFile = FileUtils.testFile();
        segment = open(testFile);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(segment);
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void entries_returns_total_entries_of_all_blocks() {
        segment.add("a");
        segment.add("b");

        assertEquals(2, segment.entries());
    }

    @Test
    public void entries_is_correct_after_reopening_segment() {
        segment.add("a");
        segment.add("b");

        segment.writeBlock();
        segment.close();
        segment = open(testFile);
        assertEquals(2, segment.entries());
    }

    @Test
    public void flush_writes_block() {
        segment.add("a");
        segment.flush();

        segment.close();
        segment = open(testFile);
        assertEquals(1, segment.entries());
    }

    @Test
    public void roll_writes_block() {
        segment.add("a");
        segment.roll(1);

        segment.close();
        segment = open(testFile);
        assertEquals(1, segment.entries());
    }
}