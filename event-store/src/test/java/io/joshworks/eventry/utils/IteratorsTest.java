package io.joshworks.eventry.utils;

import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.iterators.Iterators;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IteratorsTest {

    @Test
    public void reversed() {
    }

    @Test
    public void concat() {
        CloseableIterator<Integer> first = Iterators.of(List.of(1));
        CloseableIterator<Integer> second = Iterators.of(List.of(2));

        CloseableIterator<Integer> concat = Iterators.concat(List.of(first, second));

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(1), concat.next());

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(2), concat.next());

        assertFalse(concat.hasNext());

    }

    @Test
    public void concat_empty() {
        CloseableIterator<Integer> first = Iterators.empty();
        CloseableIterator<Integer> second = Iterators.empty();

        Iterator<Integer> concat = Iterators.concat(List.of(first, second));

        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());

    }

    @Test
    public void concat_one_of_lists_empty() {
        CloseableIterator<Integer> first = Iterators.of(List.of(1));
        CloseableIterator<Integer> second = Iterators.empty();

        Iterator<Integer> concat = Iterators.concat(List.of(first, second));

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(1), concat.next());

        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
    }

    @Test
    public void concat_one_of_second_item_empty() {
        CloseableIterator<Integer> first = Iterators.of(List.of(1));
        CloseableIterator<Integer> second = Iterators.empty();
        CloseableIterator<Integer> third = Iterators.of(List.of(2));

        Iterator<Integer> concat = Iterators.concat(List.of(first, second, third));

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(1), concat.next());

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(2), concat.next());

        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());

    }

    @Test
    public void ordered() {
        //must be ordered
        List<User> users1 = List.of(new User(1, "a"), new User(3, "b"), new User(5, "c"), new User(7, "d"));
        List<User> users2 = List.of(new User(2, "e"), new User(3, "f"), new User(6, "g"), new User(8, "h"));

        List<CloseableIterator<User>> compose = List.of(Iterators.of(users1), Iterators.of(users2));

        CloseableIterator<User> ordered = Iterators.ordered(compose, User::age);

        assertEquals("a", ordered.next().name);
        assertEquals("e", ordered.next().name);
        assertEquals("f", ordered.next().name);
        assertEquals("b", ordered.next().name);
        assertEquals("c", ordered.next().name);
        assertEquals("g", ordered.next().name);
        assertEquals("d", ordered.next().name);
        assertEquals("h", ordered.next().name);

    }


    private static class User {
        private final int age;
        private final String name;

        private User(int age, String name) {
            this.age = age;
            this.name = name;
        }

        public int age() {
            return age;
        }

        public String name() {
            return name;
        }
    }

}