package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.Range;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;


public abstract class TreeIteratorBase {

    protected abstract BPlusTree<Integer, String> create(int order);

    @Test
    public void iterator() {
        BPlusTree<Integer, String> tree = create(3);

        int size = 100;
        int startKey = 0;
        int endKey = 99;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        assertIterator(tree.iterator(), size, startKey, endKey);
    }

    @Test
    public void range() {
        BPlusTree<Integer, String> tree = create(3);

        for (int i = 0; i < 400; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 100;
        int endKey = 400;
        int expectedSize = endKey - startKey;
        int expectedLastKey = endKey - 1;

        Iterator<Entry<Integer, String>> iterator = tree.iterator(Range.of(Integer.class).startInclusive(100).endExclusive(400));
        assertIterator(iterator, expectedSize, startKey, expectedLastKey);
    }

    @Test
    public void range_out_of_range() {
        BPlusTree<Integer, String> tree = create(3);

        int size = 102;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 100;
        int expectedSize = 2;
        int expectedLastKey = 101;

        Iterator<Entry<Integer, String>> iterator = tree.iterator(Range.of(Integer.class).startInclusive(100).endExclusive(400));
        assertIterator(iterator, expectedSize, startKey, expectedLastKey);
    }

    @Test
    public void limit_skip() {
        BPlusTree<Integer, String> tree = create(3);

        int size = 500;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 100;
        int expectedSize = 400;
        int expectedLastKey = 499;

        Iterator<Entry<Integer, String>> iterator = tree.iterator(Range.of(Integer.class).skip(100).limit(999));
        assertIterator(iterator, expectedSize, startKey, expectedLastKey);

    }

    @Test
    public void limit_limit() {
        BPlusTree<Integer, String> tree = create(3);

        int size = 500;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 0;
        int expectedSize = 10;
        int expectedLastKey = 9;
        Iterator<Entry<Integer, String>> iterator = tree.iterator(Range.of(Integer.class).skip(0).limit(10));
        assertIterator(iterator, expectedSize, startKey, expectedLastKey);
    }

    @Test
    public void startInclusive_limit() {
        BPlusTree<Integer, String> tree = create(3);

        int size = 500;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 450;
        int expectedSize = 10;
        int expectedLastKey = 459;

        Iterator<Entry<Integer, String>> iterator = tree.iterator(Range.of(Integer.class).startInclusive(450).skip(0).limit(10));
        assertIterator(iterator, expectedSize, startKey, expectedLastKey);

    }

    @Test
    public void startInclusive_limit_skip() {
        BPlusTree<Integer, String> tree = create(3);

        int size = 500;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 455;
        int expectedSize = 10;
        int expectedLastKey = 464;

        Iterator<Entry<Integer, String>> iterator = tree.iterator(Range.of(Integer.class).startInclusive(450).skip(5).limit(10));
        assertIterator(iterator, expectedSize, startKey, expectedLastKey);

    }

    private void assertIterator(Iterator<Entry<Integer, String>> iterator, int expectedSize, int expectedFirstKey, int expectedLastKey) {
        long last = expectedFirstKey - 1;
        int count = 0;
        while (iterator.hasNext()) {
            Entry<Integer, String> next = iterator.next();
            System.out.println(next);
            assertThat(next.key, greaterThan((int) last));
            assertThat(next.key, greaterThanOrEqualTo(expectedFirstKey));
            assertThat(next.key, lessThanOrEqualTo(expectedLastKey));
            last = next.key;
            count++;
        }

        assertEquals(expectedSize, count);
        assertEquals(expectedLastKey, last);
    }
}