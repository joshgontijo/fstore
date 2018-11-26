package io.joshworks.fstore.log.appender.level;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.header.Type;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class LevelsTest {

    @Test
    public void segments_return_only_level_segments() {

        DummySegment zero = new DummySegment(0, Type.LOG_HEAD);
        DummySegment seg1 = new DummySegment(1, Type.READ_ONLY);
        DummySegment seg11 = new DummySegment(1, Type.READ_ONLY);
        DummySegment seg2 = new DummySegment(2, Type.READ_ONLY);
        DummySegment seg3 = new DummySegment(3, Type.READ_ONLY);

        Levels<String> levels = Levels.create(
                List.of(
                        zero,
                        seg1,
                        seg11,
                        seg2,
                        seg3));


        List<Log<String>> segments = levels.getSegments(1);
        assertEquals(2, segments.size());
        assertEquals(seg1, segments.get(0));
        assertEquals(seg11, segments.get(1));

        segments = levels.getSegments(2);
        assertEquals(1, segments.size());
        assertEquals(seg2, segments.get(0));

        segments = levels.getSegments(3);
        assertEquals(1, segments.size());
        assertEquals(seg3, segments.get(0));


    }

    @Test
    public void segments_return_sorted_for_single_level() {

        DummySegment zero = new DummySegment(0, Type.LOG_HEAD);
        DummySegment seg1 = new DummySegment(1, Type.READ_ONLY);
        DummySegment seg2 = new DummySegment(1, Type.READ_ONLY);
        DummySegment seg3 = new DummySegment(1, Type.READ_ONLY);

        Levels<String> levels = Levels.create(
                List.of(
                        zero,
                        seg1,
                        seg2,
                        seg3));

        List<Log<String>> segments = levels.getSegments(1);
        assertEquals(3, segments.size());

        assertEquals(seg1, segments.get(0));
        assertEquals(seg2, segments.get(1));
        assertEquals(seg3, segments.get(2));
    }

    @Test
    public void segments_return_FORWARD_segments_order_for_multiple_levels() {
        DummySegment zero = new DummySegment(0, Type.LOG_HEAD);
        DummySegment seg1 = new DummySegment(1, Type.READ_ONLY);
        DummySegment seg2 = new DummySegment(2, Type.READ_ONLY);
        DummySegment seg3 = new DummySegment(3, Type.READ_ONLY);

        Levels<String> levels = Levels.create(
                List.of(
                        zero,
                        seg1,
                        seg2,
                        seg3));


        List<Log<String>> segments = levels.getSegments(Direction.FORWARD);

        assertEquals(4, segments.size()); //zero included

        assertEquals(seg3, segments.get(0));
        assertEquals(seg2, segments.get(1));
        assertEquals(seg1, segments.get(2));
        assertEquals(zero, segments.get(3));
    }

    @Test
    public void segments_return_FORWARD_ordered_segments_on_multiple_levels() {
        DummySegment zero = new DummySegment(0, Type.LOG_HEAD);
        DummySegment seg11 = new DummySegment("seg11", 1, Type.READ_ONLY);
        DummySegment seg12 = new DummySegment("seg12", 1, Type.READ_ONLY);
        DummySegment seg21 = new DummySegment("seg21", 2, Type.READ_ONLY);
        DummySegment seg22 = new DummySegment("seg22", 2, Type.READ_ONLY);
        DummySegment seg31 = new DummySegment("seg31", 3, Type.READ_ONLY);
        DummySegment seg32 = new DummySegment("seg32", 3, Type.READ_ONLY);

        Levels<String> levels = Levels.create(
                List.of(
                        zero,
                        seg11,
                        seg12,
                        seg21,
                        seg22,
                        seg31,
                        seg32));

        List<Log<String>> segments = levels.getSegments(Direction.FORWARD);

        assertEquals(7, segments.size()); //zero included

        assertEquals(seg31, segments.get(0));
        assertEquals(seg32, segments.get(1));
        assertEquals(seg21, segments.get(2));
        assertEquals(seg22, segments.get(3));
        assertEquals(seg11, segments.get(4));
        assertEquals(seg12, segments.get(5));
        assertEquals(zero, segments.get(6));
    }

    @Test
    public void segments_return_BACKWARD_ordered_segments_on_multiple_levels() {
        DummySegment zero = new DummySegment(0, Type.LOG_HEAD);
        DummySegment seg11 = new DummySegment("seg11", 1, Type.READ_ONLY);
        DummySegment seg12 = new DummySegment("seg12", 1, Type.READ_ONLY);
        DummySegment seg21 = new DummySegment("seg21", 2, Type.READ_ONLY);
        DummySegment seg22 = new DummySegment("seg22", 2, Type.READ_ONLY);
        DummySegment seg31 = new DummySegment("seg31", 3, Type.READ_ONLY);
        DummySegment seg32 = new DummySegment("seg32", 3, Type.READ_ONLY);

        Levels<String> levels = Levels.create(
                List.of(
                        zero,
                        seg11,
                        seg12,
                        seg21,
                        seg22,
                        seg31,
                        seg32));

        List<Log<String>> segments = levels.getSegments(Direction.BACKWARD);

        assertEquals(7, segments.size()); //zero included

        assertEquals(zero, segments.get(0));
        assertEquals(seg12, segments.get(1));
        assertEquals(seg11, segments.get(2));
        assertEquals(seg22, segments.get(3));
        assertEquals(seg21, segments.get(4));
        assertEquals(seg32, segments.get(5));
        assertEquals(seg31, segments.get(6));
    }

    @Test
    public void get_return_segment_for_given_index() {
        DummySegment zero = new DummySegment(0, Type.LOG_HEAD);
        DummySegment seg11 = new DummySegment("seg11", 1, Type.READ_ONLY);
        DummySegment seg12 = new DummySegment("seg12", 1, Type.READ_ONLY);
        DummySegment seg21 = new DummySegment("seg21", 2, Type.READ_ONLY);
        DummySegment seg22 = new DummySegment("seg22", 2, Type.READ_ONLY);
        DummySegment seg31 = new DummySegment("seg31", 3, Type.READ_ONLY);
        DummySegment seg32 = new DummySegment("seg32", 3, Type.READ_ONLY);

        Levels<String> levels = Levels.create(
                List.of(
                        zero,
                        seg11,
                        seg12,
                        seg21,
                        seg22,
                        seg31,
                        seg32));


        assertEquals(zero, levels.get(6));
        assertEquals(seg12, levels.get(5));
        assertEquals(seg11, levels.get(4));
        assertEquals(seg22, levels.get(3));
        assertEquals(seg21, levels.get(2));
        assertEquals(seg32, levels.get(1));
        assertEquals(seg31, levels.get(0));
    }

    @Test(expected = IllegalStateException.class)
    public void exception_is_thrown_when_creating_levels_without_LOG_HEAD_segment() {
        Levels.create(List.of(new DummySegment(0, Type.READ_ONLY)));
    }

    @Test(expected = IllegalStateException.class)
    public void exception_is_thrown_when_creating_levels_without_level_zero_segment() {
        Levels.create(List.of(new DummySegment(1, Type.LOG_HEAD)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void exception_is_thrown_when_appending_new_segment_that_is_not_level_zero() {
        Levels<String> levels = Levels.create(List.of(new DummySegment(0, Type.LOG_HEAD)));
        levels.appendSegment(new DummySegment(1, Type.LOG_HEAD));
    }

    @Test
    public void appending_maintains_ordering() {
        DummySegment seg1 = new DummySegment("seg1", 0, Type.LOG_HEAD);
        DummySegment seg2 = new DummySegment("seg2", 0, Type.READ_ONLY);
        DummySegment seg3 = new DummySegment("seg3", 0, Type.READ_ONLY);

        Levels<String> levels = Levels.create(List.of(seg1));

        seg1.roll(1);
        levels.appendSegment(seg2);

        assertEquals(seg1, levels.get(0));
        assertEquals(1, ((DummySegment) levels.get(0)).level);

        //the new level zero
        assertEquals(seg2, levels.get(1));
        assertEquals(0, ((DummySegment) levels.get(1)).level);


        levels.current().roll(1);
        levels.appendSegment(seg3);

        assertEquals(seg1, levels.get(0));
        assertEquals(1, ((DummySegment) levels.get(0)).level);

        assertEquals(seg2, levels.get(1));
        assertEquals(1, ((DummySegment) levels.get(1)).level);

        //the new level zero
        assertEquals(seg3, levels.get(2));
        assertEquals(0, ((DummySegment) levels.get(2)).level);

    }

    @Test
    public void merge_maintains_ordering() {
        DummySegment seg1 = new DummySegment("seg1", 0, Type.LOG_HEAD);
        DummySegment seg2 = new DummySegment("seg2", 0, Type.LOG_HEAD);
        DummySegment seg3 = new DummySegment("seg3", 0, Type.LOG_HEAD);
        DummySegment seg4 = new DummySegment("seg4", 0, Type.LOG_HEAD);

        DummySegment seg5 = new DummySegment("seg5", 2, Type.READ_ONLY);

        Levels<String> levels = Levels.create(List.of(seg1));

        seg1.roll(1);
        levels.appendSegment(seg2);

        seg2.roll(1);
        levels.appendSegment(seg3);

        seg3.roll(1);
        levels.appendSegment(seg4);

        levels.merge(List.of(seg1, seg2, seg3), seg5);


        assertEquals(seg5, levels.get(0));
        assertEquals(2, ((DummySegment) levels.get(0)).level);

        assertEquals(seg4, levels.get(1));
        assertEquals(0, ((DummySegment) levels.get(1)).level);

    }


    private static final class DummySegment implements Log<String> {

        private int level;
        private final String name;
        private Type type;
        private final long createdDate;
        private boolean readOnly;

        private DummySegment(int level, Type type) {
            this(UUID.randomUUID().toString().substring(0, 8), level, type);
        }

        private DummySegment(String name, int level, Type type) {
            this.level = level;
            this.name = name;
            this.type = type;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.createdDate = System.currentTimeMillis();
        }

        @Override
        public String name() {
            return null;
        }


        @Override
        public SegmentIterator<String> iterator(long position, Direction direction) {
            return null;
        }

        @Override
        public SegmentIterator<String> iterator(Direction direction) {
            return null;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public String get(long position) {
            return null;
        }

        @Override
        public long fileSize() {
            return 0;
        }

        @Override
        public long logicalSize() {
            return 0;
        }

        @Override
        public SegmentState rebuildState(long lastKnownPosition) {
            return null;
        }

        @Override
        public void delete() {

        }

        @Override
        public void roll(int level) {
            this.level = level;
            this.readOnly = true;
        }

        @Override
        public boolean readOnly() {
            return readOnly;
        }

        @Override
        public long entries() {
            return 0;
        }

        @Override
        public int level() {
            return level;
        }

        @Override
        public long created() {
            return createdDate;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public long append(String data) {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void flush() {

        }

        @Override
        public String toString() {
            return "level=" + level + ", name='" + name + '\'';
        }
    }


}