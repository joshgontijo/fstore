package io.joshworks.es2.directory;


import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ViewTest {


    @Test
    public void testView() {
        var items = List.of(
                new DummySegment(0,1),
                new DummySegment(0,2),
                new DummySegment(1,0)
        );

        var view = new View<>(items, LoggerFactory.getLogger("test"));

        assertEquals(new SegmentId(0,2),  view.head().segmentId);
        assertEquals(new SegmentId(0,1),  view.get(1).segmentId);
        assertEquals(new SegmentId(1,0),  view.get(2).segmentId);

    }


    public static class DummySegment implements SegmentFile {
        private final SegmentId segmentId;

        public DummySegment(int level, long idx) {
            this.segmentId = new SegmentId(level, idx);
        }

        @Override
        public void close() {

        }

        @Override
        public void delete() {

        }

        @Override
        public String name() {
            return segmentId.toString();
        }

        @Override
        public long size() {
            return 0;
        }
    }
}