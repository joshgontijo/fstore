package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;
import io.joshworks.fstore.lsmtree.utils.InMemorySegment;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.joshworks.fstore.lsmtree.sstable.entry.Entry.NO_MAX_AGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SSTableCompactorTest {

    @Test
    public void result_segment_is_ordered_after_merging_segments_with_smaller_entries() {

        SSTableCompactor<String, Integer> compactor = new SSTableCompactor<>(NO_MAX_AGE);

        List<Entry<String, Integer>> segment1 = List.of(Entry.add("3", 3), Entry.add("4", 4));
        List<Entry<String, Integer>> segment2 = List.of(Entry.delete("1"), Entry.delete("2"));

        InMemorySegment<Entry<String, Integer>> memSeg = new InMemorySegment<>();
        compactor.mergeItems(toIt(segment1, segment2), memSeg);

        assertEquals("1", memSeg.records.get(0).key);
        assertEquals("2", memSeg.records.get(1).key);
        assertEquals("3", memSeg.records.get(2).key);
        assertEquals("4", memSeg.records.get(3).key);
    }

    @Test
    public void only_last_entry_of_same_key_is_kept() {

        SSTableCompactor<String, Integer> compactor = new SSTableCompactor<>(NO_MAX_AGE);

        List<Entry<String, Integer>> segment1 = List.of(Entry.add("1", 1), Entry.add("2", 2));
        List<Entry<String, Integer>> segment2 = List.of(Entry.add("1", 1111), Entry.add("2", 2222));

        InMemorySegment<Entry<String, Integer>> memSeg = new InMemorySegment<>();
        compactor.mergeItems(toIt(segment1, segment2), memSeg);

        assertEquals(Integer.valueOf(1111), memSeg.records.get(0).value);
        assertEquals(Integer.valueOf(2222), memSeg.records.get(1).value);
    }

    @Test
    public void deleted_entry_is_kept() {

        SSTableCompactor<String, Integer> compactor = new SSTableCompactor<>(NO_MAX_AGE);

        List<Entry<String, Integer>> segment1 = List.of(Entry.add("1", 1), Entry.add("2", 2));
        List<Entry<String, Integer>> segment2 = List.of(Entry.delete("1"), Entry.delete("2"));

        InMemorySegment<Entry<String, Integer>> memSeg = new InMemorySegment<>();
        compactor.mergeItems(toIt(segment1, segment2), memSeg);

        assertEquals(2, memSeg.records.size());

        assertNull(memSeg.records.get(0).value);
        assertNull(memSeg.records.get(1).value);
    }


    @SafeVarargs
    private static List<PeekingIterator<Entry<String, Integer>>> toIt(List<Entry<String, Integer>>... items) {
        return Arrays.stream(items)
                .map(Iterators::of)
                .map(Iterators::peekingIterator)
                .collect(Collectors.toList());
    }
}