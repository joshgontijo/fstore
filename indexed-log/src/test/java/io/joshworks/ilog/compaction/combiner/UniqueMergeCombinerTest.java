package io.joshworks.ilog.compaction.combiner;


import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.LogUtil;
import io.joshworks.ilog.RecordUtils;
import io.joshworks.ilog.SegmentIterator;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.joshworks.ilog.RecordUtils.create;
import static org.junit.Assert.assertEquals;

public class UniqueMergeCombinerTest {

    private static final int INDEX_LENGTH = Size.MB.ofInt(1);
    private static final RecordPool pool = RecordPool.create().build();
    private static final RowKey rowKey = RowKey.LONG;
    private final List<IndexedSegment> segments = new ArrayList<>();

    private final Random random = new Random(123L);

    @After
    public void tearDown() {
        for (IndexedSegment segment : segments) {
            segment.delete();
        }
    }

    @Test
    public void merge_three_segments() {

        IndexedSegment seg1 = segmentWith("a", "c", "d");
        IndexedSegment seg2 = segmentWith("a", "b", "c");
        IndexedSegment seg3 = segmentWith("a", "b", "e");
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);

        Records result = readAll(out);

        assertRecord("a", result.get(0));
        assertRecord("b", result.get(1));
        assertRecord("c", result.get(2));
        assertRecord("d", result.get(3));
        assertRecord("e", result.get(4));
    }

    @Test
    public void merge_three_segments_many_items() {

        IndexedSegment seg1 = segmentWith("b", "c", "d", "e", "j", "k", "o", "p");
        IndexedSegment seg2 = segmentWith("a", "b", "c", "z");
        IndexedSegment seg3 = segmentWith("a", "b", "e", "f", "k", "o", "p", "z");
        IndexedSegment seg4 = segmentWith("a", "c", "e", "f", "k", "o", "p", "z");
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2, seg3, seg4), out);

        Records result = readAll(out);

        assertRecord("a", result.get(0));
        assertRecord("b", result.get(1));
        assertRecord("c", result.get(2));
        assertRecord("d", result.get(3));
        assertRecord("e", result.get(4));
    }

    @Test
    public void segment_with_first_duplicates_is_not_removed_from_set() {

        IndexedSegment seg1 = segmentWith("a", "d");
        IndexedSegment seg2 = segmentWith("a", "b", "c");
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2), out);

        Records result = readAll(out);

        assertRecord("a", result.get(0));
        assertRecord("b", result.get(1));
        assertRecord("c", result.get(2));
        assertRecord("d", result.get(3));
    }

    @Test
    public void merge() {

        IndexedSegment seg1 = segmentWith("b", "c", "d");
        IndexedSegment seg2 = segmentWith("a", "b", "c");
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2), out);

        Records result = readAll(out);

        assertRecord("a", result.get(0));
        assertRecord("b", result.get(1));
        assertRecord("c", result.get(2));
        assertRecord("d", result.get(3));
    }

    @Test
    public void when_duplicated_entry_keep_newest() {

        IndexedSegment seg1 = segmentWith(create(1, "a"));
        IndexedSegment seg2 = segmentWith(create(1, "b"));
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2), out);

        Records result = readAll(out);

        assertEquals(1, result.size());
        assertRecord("b", result.get(0));
    }

    @Test
    public void the_subsequent_entries_of_duplicated_entry_in_the_first_segment_are_kept() {

        IndexedSegment seg1 = segmentWith(create(1, "a"), create(2, "b"), create(3, "c"));
        IndexedSegment seg2 = segmentWith(create(1, "d"));
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2), out);

        Records result = readAll(out);

        assertEquals(3, result.size());
        assertRecord("d", result.get(0));
        assertRecord("b", result.get(1));
        assertRecord("c", result.get(2));
    }

    @Test
    public void the_subsequent_entries_of_duplicated_entry_in_the_second_segment_are_kept() {

        IndexedSegment seg1 = segmentWith(create(1, "a"));
        IndexedSegment seg2 = segmentWith(create(1, "b"), create(2, "c"), create(3, "d"));
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2), out);

        Records result = readAll(out);

        assertEquals(3, result.size());
        assertRecord("b", result.get(0));
        assertRecord("c", result.get(1));
        assertRecord("d", result.get(2));
    }

    @Test
    public void when_duplicated_entry_keep_newest_multiple_segments() {

        IndexedSegment seg1 = segmentWith(create(1, "a"));
        IndexedSegment seg2 = segmentWith(create(1, "b"));
        IndexedSegment seg3 = segmentWith(create(1, "c"));
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);

        Records result = readAll(out);

        assertEquals(1, result.size());
        assertRecord("c", result.get(0));
    }

    @Test
    public void when_duplicated_entry_keep_newest_multiple_segments_2() {

        IndexedSegment seg1 = segmentWith(create(1, "a"), create(2, "b"));
        IndexedSegment seg2 = segmentWith(create(1, "c"));
        IndexedSegment seg3 = segmentWith(create(3, "d"));
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2, seg3), out);

        Records result = readAll(out);

        assertEquals(3, result.size());
        assertRecord("c", result.get(0));
        assertRecord("b", result.get(1));
        assertRecord("d", result.get(2));
    }

    @Test
    public void when_duplicated_entry_keep_newest_multiple_segments_3() {

        IndexedSegment seg1 = segmentWith(create(1, "a"), create(2, "b"));
        IndexedSegment seg2 = segmentWith(create(1, "c"), create(5, "d"));
        IndexedSegment seg3 = segmentWith(create(3, "e"));
        IndexedSegment seg4 = segmentWith(create(1, "f"), create(2, "g"), create(6, "h"));
        IndexedSegment out = createSegment();

        SegmentCombiner combiner = new UniqueMergeCombiner(pool, rowKey);

        combiner.merge(Arrays.asList(seg1, seg2, seg3, seg4), out);

        Records results = readAll(out);

        assertEquals(5, results.size());
        assertRecord("f", results.get(0));
        assertRecord("g", results.get(1));
        assertRecord("e", results.get(2));
        assertRecord("d", results.get(3));
        assertRecord("h", results.get(4));
    }

    private IndexedSegment segmentWith(String... values) {
        try {
            Records records = pool.empty();
            for (String value : values) {
                records.add(RecordUtils.create(value.hashCode(), value));
            }

            IndexedSegment segment = createSegment();
            segment.write(records, 0);
            segment.roll();
            return segment;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private IndexedSegment segmentWith(Record... items) {
        try {
            IndexedSegment segment = createSegment();
            Records records = pool.empty();
            for (Record value : items) {
                records.add(value);
            }
            segment.write(records, 0);
            segment.roll();
            return segment;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertRecord(String expectedVal, Record record) {
        String val = RecordUtils.stringValue(record);
        assertEquals(expectedVal, val);
    }

    private IndexedSegment createSegment() {
        File file = TestUtils.testFile(LogUtil.segmentFileName(System.nanoTime(), random.nextInt()));
        IndexedSegment segment = new IndexedSegment(file, INDEX_LENGTH, rowKey, pool);
        segments.add(segment);
        return segment;
    }

    private Records readAll(IndexedSegment segment) {
        try (SegmentIterator it = new SegmentIterator(segment, Log.START, 4096, pool)) {
            Records records = pool.empty();
            while (it.hasNext()) {
                Record rec = it.next();
                records.add(rec);
                System.out.println(RecordUtils.toString(rec));
            }
            return records;
        }
    }


}