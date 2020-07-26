package io.joshworks.es;

import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SegmentDirectoryTest {

    private File root;
    private SegmentDirectory<DummySegment> directory;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        directory = new SegmentDirectory<>(root, "dummy", 10);
    }

    @Test
    public void headIdx_returns_negative_when_no_file() {
        assertEquals(-1L, directory.headIdx());
    }

    @Test
    public void head_returns_null_when_no_file() {
        assertNull(directory.head());
    }

    @Test
    public void head_returns_lastItem() {
        File head = directory.newHeadFile();
        directory.add(new DummySegment(head));

        assertEquals(head, directory.head().file);
    }

    @Test
    public void adding_a_new_segment_replaces_head() {
        File f1 = directory.newHeadFile();
        directory.add(new DummySegment(f1));

        assertEquals(f1, directory.head().file);
        assertEquals(0, directory.headIdx());


        File f2 = directory.newHeadFile();
        directory.add(new DummySegment(f2));

        assertEquals(f2, directory.head().file);
        assertEquals(1, directory.headIdx());

    }

    @Test
    public void loadSegments_loads_all_files() {
        File f1 = directory.newHeadFile();
        directory.add(new DummySegment(f1));

        File f2 = directory.newHeadFile();
        directory.add(new DummySegment(f2));

        directory.close();

        directory.loadSegments(DummySegment::new);

        assertEquals(f2, directory.head().file);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannot_merge_head() {
        directory.add(new DummySegment(directory.newHeadFile()));
        directory.add(new DummySegment(directory.newHeadFile()));
        Set<DummySegment> toBeMerged = Set.of(
                directory.getSegment(0),
                directory.getSegment(1));

        directory.newMergeFile(toBeMerged);
    }

    @Test
    public void merge_sequential_files() {
        DummySegment seg0 = new DummySegment(directory.newHeadFile());
        directory.add(seg0);

        DummySegment seg1 = new DummySegment(directory.newHeadFile());
        directory.add(seg1);

        DummySegment seg2 = new DummySegment(directory.newHeadFile());
        directory.add(seg2);

        Set<DummySegment> toBeMerged = Set.of(
                directory.getSegment(0),
                directory.getSegment(1));

        File resultFile = directory.newMergeFile(toBeMerged);
        DummySegment result = new DummySegment(resultFile);

        directory.merge(result, toBeMerged);

        assertEquals(2, directory.size());
        assertEquals(2, directory.headIdx());

        assertNull(directory.tryGet(1));
        assertTrue(seg0.deleted);
        assertTrue(seg1.deleted);
    }

    private static class DummySegment implements SegmentFile {

        private final File file;
        private boolean closed;
        private boolean deleted;

        public DummySegment(File file) {
            FileUtils.createIfNotExists(file);
            this.file = file;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void delete() {
            deleted = true;
            FileUtils.deleteIfExists(file);
        }

        @Override
        public File file() {
            return file;
        }
    }

}