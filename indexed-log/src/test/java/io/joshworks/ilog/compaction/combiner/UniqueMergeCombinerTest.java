//package io.joshworks.ilog.compaction.combiner;
//
//
//import io.joshworks.fstore.core.io.buffers.BufferPool;
//import io.joshworks.fstore.core.util.Size;
//import io.joshworks.fstore.core.util.TestUtils;
//import io.joshworks.fstore.serializer.Serializers;
//import io.joshworks.ilog.IndexedSegment;
//import io.joshworks.ilog.LogUtil;
//import io.joshworks.ilog.RecordBatchIterator;
//import io.joshworks.ilog.RecordUtils;
//import io.joshworks.ilog.index.KeyComparator;
//import org.junit.After;
//import org.junit.Test;
//
//import java.io.File;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Random;
//
//import static org.junit.Assert.assertEquals;
//
//public class UniqueMergeCombinerTest {
//
//    private static final int INDEX_LENGTH = Size.MB.ofInt(1);
//    private static final BufferPool pool = BufferPool.unpooled(4096, false);
//    private static final KeyComparator comparator = KeyComparator.LONG;
//    private final List<IndexedSegment> segments = new ArrayList<>();
//
//    private final Random random = new Random(123L);
//
//    @After
//    public void tearDown() {
//        for (IndexedSegment segment : segments) {
//            segment.delete();
//        }
//    }
//
//    @Test
//    public void merge_three_segments() {
//
//        IndexedSegment seg1 = segmentWith("a", "c", "d");
//        IndexedSegment seg2 = segmentWith("a", "b", "c");
//        IndexedSegment seg3 = segmentWith("a", "b", "e");
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertRecord("a", result.get(0));
//        assertRecord("b", result.get(1));
//        assertRecord("c", result.get(2));
//        assertRecord("d", result.get(3));
//        assertRecord("e", result.get(4));
//    }
//
//    @Test
//    public void merge_three_segments_many_items() {
//
//        IndexedSegment seg1 = segmentWith("b", "c", "d", "e", "j", "k", "o", "p");
//        IndexedSegment seg2 = segmentWith("a", "b", "c", "z");
//        IndexedSegment seg3 = segmentWith("a", "b", "e", "f", "k", "o", "p", "z");
//        IndexedSegment seg4 = segmentWith("a", "c", "e", "f", "k", "o", "p", "z");
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2, seg3, seg4), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertRecord("a", result.get(0));
//        assertRecord("b", result.get(1));
//        assertRecord("c", result.get(2));
//        assertRecord("d", result.get(3));
//        assertRecord("e", result.get(4));
//    }
//
//    @Test
//    public void segment_with_first_duplicates_is_not_removed_from_set() {
//
//        IndexedSegment seg1 = segmentWith("a", "d");
//        IndexedSegment seg2 = segmentWith("a", "b", "c");
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertRecord("a", result.get(0));
//        assertRecord("b", result.get(1));
//        assertRecord("c", result.get(2));
//        assertRecord("d", result.get(3));
//    }
//
//    @Test
//    public void merge() {
//
//        IndexedSegment seg1 = segmentWith("b", "c", "d");
//        IndexedSegment seg2 = segmentWith("a", "b", "c");
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertRecord("a", result.get(0));
//        assertRecord("b", result.get(1));
//        assertRecord("c", result.get(2));
//        assertRecord("d", result.get(3));
//    }
//
//    @Test
//    public void when_duplicated_entry_keep_newest() {
//
//        IndexedSegment seg1 = segmentWith(of(1, "a"));
//        IndexedSegment seg2 = segmentWith(of(1, "b"));
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertEquals(1, result.size());
//        assertRecord("b", result.get(0));
//    }
//
//    @Test
//    public void the_subsequent_entries_of_duplicated_entry_in_the_first_segment_are_kept() {
//
//        IndexedSegment seg1 = segmentWith(of(1, "a"), of(2, "b"), of(3, "c"));
//        IndexedSegment seg2 = segmentWith(of(1, "d"));
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertEquals(3, result.size());
//        assertRecord("d", result.get(0));
//        assertRecord("b", result.get(1));
//        assertRecord("c", result.get(2));
//    }
//
//    @Test
//    public void the_subsequent_entries_of_duplicated_entry_in_the_second_segment_are_kept() {
//
//        IndexedSegment seg1 = segmentWith(of(1, "a"));
//        IndexedSegment seg2 = segmentWith(of(1, "b"), of(2, "c"), of(3, "d"));
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertEquals(3, result.size());
//        assertRecord("b", result.get(0));
//        assertRecord("c", result.get(1));
//        assertRecord("d", result.get(2));
//    }
//
//    @Test
//    public void when_duplicated_entry_keep_newest_multiple_segments() {
//
//        IndexedSegment seg1 = segmentWith(of(1, "a"));
//        IndexedSegment seg2 = segmentWith(of(1, "b"));
//        IndexedSegment seg3 = segmentWith(of(1, "c"));
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertEquals(1, result.size());
//        assertRecord("c", result.get(0));
//    }
//
//    @Test
//    public void when_duplicated_entry_keep_newest_multiple_segments_2() {
//
//        IndexedSegment seg1 = segmentWith(of(1, "a"), of(2, "b"));
//        IndexedSegment seg2 = segmentWith(of(1, "c"));
//        IndexedSegment seg3 = segmentWith(of(3, "d"));
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);
//
//        List<ByteBuffer> result = readAll(out);
//
//        assertEquals(3, result.size());
//        assertRecord("c", result.get(0));
//        assertRecord("b", result.get(1));
//        assertRecord("d", result.get(2));
//    }
//
//    @Test
//    public void when_duplicated_entry_keep_newest_multiple_segments_3() {
//
//        IndexedSegment seg1 = segmentWith(of(1, "a"), of(2, "b"));
//        IndexedSegment seg2 = segmentWith(of(1, "c"), of(5, "d"));
//        IndexedSegment seg3 = segmentWith(of(3, "e"));
//        IndexedSegment seg4 = segmentWith(of(1, "f"), of(2, "g"), of(6, "h"));
//        IndexedSegment out = createSegment();
//
//        SegmentCombiner combiner = new UniqueMergeCombiner(comparator, pool);
//
//        combiner.merge(Arrays.asList(seg1, seg2, seg3, seg4), out);
//
//        List<ByteBuffer> results = readAll(out);
//
//        assertEquals(5, results.size());
//        assertRecord("f", results.get(0));
//        assertRecord("g", results.get(1));
//        assertRecord("e", results.get(2));
//        assertRecord("d", results.get(3));
//        assertRecord("h", results.get(4));
//    }
//
//    private IndexedSegment segmentWith(String... values) {
//        try {
//            IndexedSegment segment = createSegment();
//            for (String value : values) {
//                segment.append(of(value.hashCode(), value));
//            }
//            segment.roll();
//            return segment;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private IndexedSegment segmentWith(ByteBuffer... records) {
//        try {
//            IndexedSegment segment = createSegment();
//            for (ByteBuffer value : records) {
//                segment.append(value);
//            }
//            segment.roll();
//            return segment;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private static ByteBuffer of(long id, String label) {
//        return RecordUtils.create(id, label);
//    }
//
//    private static void assertRecord(String expectedVal, ByteBuffer record) {
//        String val = RecordUtils.readValue(record, Serializers.STRING);
//        assertEquals(expectedVal, val);
//    }
//
//    private IndexedSegment createSegment() {
//        File file = TestUtils.testFile(LogUtil.segmentFileName(System.nanoTime(), random.nextInt()));
//        IndexedSegment segment = new IndexedSegment(file, INDEX_LENGTH, comparator);
//        segments.add(segment);
//        return segment;
//    }
//
//    private List<ByteBuffer> readAll(IndexedSegment segment) {
//        RecordBatchIterator it = new RecordBatchIterator(segment, IndexedSegment.START, pool);
////        Stream<ByteBuffer> stream = Iterators.closeableStream(new RecordBatchIterator(segment, IndexedSegment.START, pool));
//        List<ByteBuffer> items = new ArrayList<>();
//        while (it.hasNext()) {
//            ByteBuffer next = it.next();
//            System.out.println(RecordUtils.toString(next, Serializers.LONG, Serializers.STRING));
//            items.add(RecordUtils.copy(next));
//        }
//        return items;
//    }
//
//
//}