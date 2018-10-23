package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class UniqueMergeCombinerTest {

    private final List<Segment> segments = new ArrayList<>();
    private DataStream dataStream = new DataStream(new SingleBufferThreadCachedPool(false));

    @After
    public void tearDown() {
        for (Segment segment : segments) {
            segment.delete();
        }
    }

    @Test
    public void merge_three_segments() {

        Segment<String> seg1 = segmentWith("b", "c", "d");
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


    private Segment<String> segmentWith(String... values) {
        File file = FileUtils.testFile();

        long logSize = 1024;
        Storage storage = StorageProvider.raf().create(file, Log.totalSizeOf(logSize));

        Segment<String> segment = new Segment<>(storage, Serializers.VSTRING, dataStream, 1234L, Type.LOG_HEAD, logSize);
        segments.add(segment);

        for (String value : values) {
            segment.append(value);
        }
        segment.roll(0);
        return segment;
    }

    private Segment<String> outputSegment() {
        long logSize = 1024;
        File file = FileUtils.testFile();
        Storage storage = StorageProvider.raf().create(file,Log.totalSizeOf(logSize));
        Segment<String> segment = new Segment<>(storage, Serializers.VSTRING, dataStream, 1234, Type.LOG_HEAD, logSize);
        segments.add(segment);
        return segment;
    }

}