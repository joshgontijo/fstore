package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LsmTreeTest {

    private LsmTree<Integer, String> lsmtree;
    private File file;


    @Before
    public void setUp() {
        file = FileUtils.testFolder();
        lsmtree = LsmTree.open(file, Serializers.INTEGER, Serializers.VSTRING, 100);
    }

    @After
    public void tearDown() {
        lsmtree.close();
        FileUtils.tryDelete(file);
    }

    @Test
    public void put_get() {
        lsmtree.put(1, "a");
        lsmtree.put(2, "b");
        lsmtree.put(3, "c");

        assertEquals("a", lsmtree.get(1));
        assertEquals("b", lsmtree.get(2));
        assertEquals("c", lsmtree.get(3));
    }

    @Test
    public void update() {
        lsmtree.put(1, "a");
        lsmtree.put(1, "b");
        lsmtree.put(1, "c");

        assertEquals("c", lsmtree.get(1));
    }

    @Test
    public void delete() {
        lsmtree.put(1, "a");
        lsmtree.put(2, "b");
        lsmtree.remove(2);

        assertEquals("a", lsmtree.get(1));
        assertNull(lsmtree.get(2));
    }

    @Test
    public void iterator_deleted_entries() throws Exception {
        int items = 10000;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        for (int i = items - 100; i < items; i++) {
            lsmtree.remove(i);
        }

        for (int i = 0; i < items; i++) {
            lsmtree.remove(i);
        }

        try (EntryIterator<Integer, String> iterator = lsmtree.iterator()) {
            while(iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                System.out.println(entry);
            }
        }
    }

}