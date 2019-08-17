package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.VStringSerializer;
import io.joshworks.fstore.core.util.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombinerTest.TestEntry.of;
import static org.junit.Assert.assertEquals;

public class UniqueMergeCombinerTest {

    private static final double CHECKSUM_PROB = 1;
    private static final int READ_PAGE_SIZE = Memory.PAGE_SIZE;
    private static final int SEGMENT_SIZE = Memory.PAGE_SIZE * 2;
    private static final int MAX_ENTRY_SIZE = Size.MB.ofInt(1);
    private final BufferPool bufferPool = new ThreadLocalBufferPool(MAX_ENTRY_SIZE);

    private final List<Segment> segments = new ArrayList<>();

    @After
    public void tearDown() {
        for (Segment segment : segments) {
            segment.delete();
        }
    }

    @Test
    public void merge_three_segments() {

        Segment<String> seg1 = segmentWith("a", "c", "d");
        Segment<String> seg2 = segmentWith("a", "b", "c");
        Segment<String> seg3 = segmentWith("a", "b", "e");
        Segment<String> out = outputSegment();

        MergeCombiner<String> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);

        List<String> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals("a", result.get(0));
        assertEquals("b", result.get(1));
        assertEquals("c", result.get(2));
        assertEquals("d", result.get(3));
        assertEquals("e", result.get(4));
    }

    @Test
    public void merge_three_segments_many_items() {

        Segment<String> seg1 = segmentWith("b", "c", "d", "e", "j", "k", "o", "p");
        Segment<String> seg2 = segmentWith("a", "b", "c", "z");
        Segment<String> seg3 = segmentWith("a", "b", "e", "f", "k", "o", "p", "z");
        Segment<String> seg4 = segmentWith("a", "c", "e", "f", "k", "o", "p", "z");
        Segment<String> out = outputSegment();

        MergeCombiner<String> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2, seg3, seg4), out);

        List<String> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals("a", result.get(0));
        assertEquals("b", result.get(1));
        assertEquals("c", result.get(2));
        assertEquals("d", result.get(3));
        assertEquals("e", result.get(4));
    }


    @Test
    public void segment_with_first_duplicates_is_not_removed_from_set() {

        Segment<String> seg1 = segmentWith("a", "d");
        Segment<String> seg2 = segmentWith("a", "b", "c");
        Segment<String> out = outputSegment();

        MergeCombiner<String> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2), out);

        List<String> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals("a", result.get(0));
        assertEquals("b", result.get(1));
        assertEquals("c", result.get(2));
        assertEquals("d", result.get(3));
    }

    @Test
    public void merge() {

        Segment<String> seg1 = segmentWith("b", "c", "d");
        Segment<String> seg2 = segmentWith("a", "b", "c");
        Segment<String> out = outputSegment();

        MergeCombiner<String> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2), out);

        List<String> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals("a", result.get(0));
        assertEquals("b", result.get(1));
        assertEquals("c", result.get(2));
        assertEquals("d", result.get(3));
    }

    @Test
    public void when_duplicated_entry_keep_newest() {

        Segment<TestEntry> seg1 = segmentWith(of(1, "a"));
        Segment<TestEntry> seg2 = segmentWith(of(1, "b"));
        Segment<TestEntry> out = testEntryOutputSegment();

        MergeCombiner<TestEntry> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2), out);

        List<TestEntry> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals(1, result.size());
        assertEquals("b", result.get(0).label);
    }

    @Test
    public void the_subsequent_entries_of_duplicated_entry_in_the_first_segment_are_kept() {

        Segment<TestEntry> seg1 = segmentWith(of(1, "a"), of(2, "b"), of(3, "c"));
        Segment<TestEntry> seg2 = segmentWith(of(1, "d"));
        Segment<TestEntry> out = testEntryOutputSegment();

        MergeCombiner<TestEntry> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2), out);

        List<TestEntry> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals(3, result.size());
        assertEquals("d", result.get(0).label);
        assertEquals("b", result.get(1).label);
        assertEquals("c", result.get(2).label);
    }

    @Test
    public void the_subsequent_entries_of_duplicated_entry_in_the_second_segment_are_kept() {

        Segment<TestEntry> seg1 = segmentWith(of(1, "a"));
        Segment<TestEntry> seg2 = segmentWith(of(1, "b"), of(2, "c"), of(3, "d"));
        Segment<TestEntry> out = testEntryOutputSegment();

        MergeCombiner<TestEntry> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2), out);

        List<TestEntry> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals(3, result.size());
        assertEquals("b", result.get(0).label);
        assertEquals("c", result.get(1).label);
        assertEquals("d", result.get(2).label);
    }

    @Test
    public void when_duplicated_entry_keep_newest_multiple_segments() {

        Segment<TestEntry> seg1 = segmentWith(of(1, "a"));
        Segment<TestEntry> seg2 = segmentWith(of(1, "b"));
        Segment<TestEntry> seg3 = segmentWith(of(1, "c"));
        Segment<TestEntry> out = testEntryOutputSegment();

        MergeCombiner<TestEntry> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);

        List<TestEntry> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals(1, result.size());
        assertEquals("c", result.get(0).label);
    }

    @Test
    public void when_duplicated_entry_keep_newest_multiple_segments_2() {

        Segment<TestEntry> seg1 = segmentWith(of(1, "a"), of(2, "b"));
        Segment<TestEntry> seg2 = segmentWith(of(1, "c"));
        Segment<TestEntry> seg3 = segmentWith(of(3, "d"));
        Segment<TestEntry> out = testEntryOutputSegment();

        MergeCombiner<TestEntry> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);

        List<TestEntry> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals(3, result.size());
        assertEquals("c", result.get(0).label);
        assertEquals("b", result.get(1).label);
        assertEquals("d", result.get(2).label);
    }

    @Test
    public void when_duplicated_entry_keep_newest_multiple_segments_3() {

        Segment<TestEntry> seg1 = segmentWith(of(1, "a"), of(2, "b"));
        Segment<TestEntry> seg2 = segmentWith(of(1, "c"), of(5, "d"));
        Segment<TestEntry> seg3 = segmentWith(of(3, "e"));
        Segment<TestEntry> seg4 = segmentWith(of(1, "f"), of(2, "g"), of(6, "h"));
        Segment<TestEntry> out = testEntryOutputSegment();

        MergeCombiner<TestEntry> combiner = new UniqueMergeCombiner<>();

        combiner.merge(Arrays.asList(seg1, seg2, seg3, seg4), out);

        List<TestEntry> result = Iterators.closeableStream(out.iterator(Direction.FORWARD)).collect(Collectors.toList());

        assertEquals(5, result.size());
        assertEquals("f", result.get(0).label);
        assertEquals("g", result.get(1).label);
        assertEquals("e", result.get(2).label);
        assertEquals("d", result.get(3).label);
        assertEquals("h", result.get(4).label);
    }

    private Segment<String> segmentWith(String... values) {
        File file = FileUtils.testFile();

        Segment<String> segment = new Segment<>(file, StorageMode.RAF, SEGMENT_SIZE, Serializers.VSTRING, bufferPool, WriteMode.LOG_HEAD, CHECKSUM_PROB, READ_PAGE_SIZE);
        segments.add(segment);

        for (String value : values) {
            segment.append(value);
        }
        segment.roll(0, false);
        return segment;
    }

    private Segment<TestEntry> segmentWith(TestEntry... values) {
        File file = FileUtils.testFile();

        Segment<TestEntry> segment = new Segment<>(file, StorageMode.RAF, SEGMENT_SIZE, new TestEntrySerializer(), bufferPool, WriteMode.LOG_HEAD, CHECKSUM_PROB, READ_PAGE_SIZE);
        segments.add(segment);

        for (TestEntry value : values) {
            segment.append(value);
        }
        segment.roll(0, false);
        return segment;
    }

    private Segment<String> outputSegment() {
        File file = FileUtils.testFile();
        Segment<String> segment = new Segment<>(file, StorageMode.RAF, SEGMENT_SIZE, Serializers.VSTRING, bufferPool, WriteMode.LOG_HEAD, CHECKSUM_PROB, READ_PAGE_SIZE);
        segments.add(segment);
        return segment;
    }

    private Segment<TestEntry> testEntryOutputSegment() {
        File file = FileUtils.testFile();
        Segment<TestEntry> segment = new Segment<>(file, StorageMode.RAF, SEGMENT_SIZE, new TestEntrySerializer(), bufferPool, WriteMode.LOG_HEAD, CHECKSUM_PROB, READ_PAGE_SIZE);
        segments.add(segment);
        return segment;
    }


    static class TestEntry implements Comparable<TestEntry> {

        private final int id;
        private final String label;

        private TestEntry(int id, String label) {
            this.id = id;
            this.label = label;
        }

        public static TestEntry of(int id, String label) {
            return new TestEntry(id, label);
        }

        @Override
        public int compareTo(TestEntry o) {
            return id - o.id;
        }

        @Override
        public String toString() {
            return "TestEntry{" + "id=" + id +
                    ", label='" + label + '\'' +
                    '}';
        }
    }

    private static class TestEntrySerializer implements Serializer<TestEntry> {

        private final Serializer<String> stringSerializer = new VStringSerializer();

        @Override
        public void writeTo(TestEntry data, ByteBuffer dst) {
            dst.putInt(data.id);
            stringSerializer.writeTo(data.label, dst);
        }

        @Override
        public TestEntry fromBytes(ByteBuffer buffer) {
            return new TestEntry(buffer.getInt(), stringSerializer.fromBytes(buffer));
        }
    }

}