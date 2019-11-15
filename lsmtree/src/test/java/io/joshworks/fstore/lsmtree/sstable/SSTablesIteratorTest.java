package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SSTablesIteratorTest {


    @Test
    public void sequential_items() {

        List<Entry<String, Integer>> segment1 = List.of(Entry.add("1", 1), Entry.add("2", 2));
        List<Entry<String, Integer>> segment2 = List.of(Entry.add("3", 3), Entry.add("4", 4));


        SSTablesIterator<String, Integer> iterator = new SSTablesIterator<>(Direction.FORWARD, toIt(segment1, segment2));
        assertExactSequence(iterator, 1, 2, 3, 4);
    }

    @Test
    public void re_sequence() {

        List<Entry<String, Integer>> segment1 = List.of(Entry.add("3", 3), Entry.add("4", 4));
        List<Entry<String, Integer>> segment2 = List.of(Entry.add("1", 1), Entry.add("2", 2));


        SSTablesIterator<String, Integer> iterator = new SSTablesIterator<>(Direction.FORWARD, toIt(segment1, segment2));
        assertExactSequence(iterator, 1, 2, 3, 4);
    }

    @Test
    public void replace() {

        List<Entry<String, Integer>> segment1 = List.of(Entry.add("1", 1), Entry.add("2", 2));
        List<Entry<String, Integer>> segment2 = List.of(Entry.add("1", 111111), Entry.add("2", 22222));

        SSTablesIterator<String, Integer> iterator = new SSTablesIterator<>(Direction.FORWARD, toIt(segment1, segment2));
        assertExactSequence(iterator, 111111, 22222);
    }

    @Test
    public void forward_iterator_with_items_on_higher_segments() {

        List<Entry<String, Integer>> segment1 = List.of(Entry.add("3", 3), Entry.add("4", 4));
        List<Entry<String, Integer>> segment2 = List.of(Entry.add("1", 1), Entry.add("2", 2));

        SSTablesIterator<String, Integer> iterator = new SSTablesIterator<>(Direction.FORWARD, toIt(segment1, segment2));
        assertExactSequence(iterator, 1, 2, 3, 4);
    }

    private static void assertExactSequence(SSTablesIterator<String, Integer> iterator, Integer... items) {
        for (Integer expected : items) {
            Entry<String, Integer> entry = iterator.next();
            assertEquals(expected, entry.value);
        }
        assertFalse("Iterator had more items than expected", iterator.hasNext());
    }

    private static List<PeekingIterator<Entry<String, Integer>>> toIt(List<Entry<String, Integer>>... items) {
        return Arrays.stream(items)
                .map(Iterators::of)
                .map(Iterators::peekingIterator)
                .collect(Collectors.toList());
    }

}