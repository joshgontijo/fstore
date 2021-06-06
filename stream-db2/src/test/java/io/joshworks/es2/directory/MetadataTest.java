package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Set;

import static io.joshworks.es2.directory.Metadata.add;
import static io.joshworks.es2.directory.Metadata.delete;
import static io.joshworks.es2.directory.Metadata.merge;
import static org.junit.Assert.assertEquals;

public class MetadataTest {

    private File testFile;
    private Metadata metadata;

    @Before
    public void setUp() {
        testFile = TestUtils.testFile();
        metadata = new Metadata(testFile);
    }

    @After
    public void tearDown() {
        TestUtils.deleteRecursively(testFile);
    }

    @Test
    public void restore_state() {
        metadata.append(add(seg(1)));
        metadata.append(add(seg(2)));
        metadata.append(merge(seg(3), List.of(seg(1), seg(2))));

        Set<SegmentId> state = metadata.state();
        assertEquals(1, state.size());
        assertEquals(3, state.iterator().next().idx());
    }

    @Test
    public void restore_state_reopen() {
        metadata.append(add(seg(1)));
        metadata.append(add(seg(2)));
        metadata.append(merge(seg(3), List.of(seg(1), seg(2))));

        metadata.close();
        metadata = new Metadata(testFile);

        Set<SegmentId> state = metadata.state();
        assertEquals(1, state.size());
        assertEquals(3, state.iterator().next().idx());
    }

    @Test(expected = Exception.class)
    public void deleting_without_segment_throw_exception() {
        metadata.append(delete(List.of(seg(1))));
        Set<SegmentId> state = metadata.state();
    }

    @Test(expected = Exception.class)
    public void duplicate_segment_throw_exception() {
        metadata.append(add(seg(1)));
        metadata.append(add(seg(1)));
        Set<SegmentId> state = metadata.state();
    }

    private static SegmentFile seg(int id) {
        return new DummySegment(id);
    }

    private static class DummySegment implements SegmentFile {

        private final String name;

        private DummySegment(int id) {
            this.name = DirectoryUtils.segmentFileName(id, 0, "dummy").split("\\.")[0];
        }

        @Override
        public void close() {

        }

        @Override
        public void delete() {

        }

        @Override
        public String name() {
            return name;
        }
    }

}