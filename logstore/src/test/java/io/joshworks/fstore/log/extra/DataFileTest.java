package io.joshworks.fstore.log.extra;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataFileTest {

    protected DataFile<String> dataFile;
    private File testFile;

    @Before
    public void setUp() {
        testFile = FileUtils.testFile("data-file.dat");
        dataFile = DataFile.of(Serializers.STRING).mmap().open(testFile);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(dataFile);
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void add_get() {
        long pos = dataFile.add("a");
        String val = dataFile.get(pos);
        assertEquals("a", val);
    }

    @Test
    public void reopening_can_retrieve_data() {
        long pos = dataFile.add("a");

        dataFile.close();
        dataFile = DataFile.of(Serializers.STRING).open(testFile);

        String val = dataFile.get(pos);
        assertEquals("a", val);
    }

    @Test
    public void size_returns_segment_write_position_instead_file_length() {
        assertEquals(LogHeader.BYTES, dataFile.length());
        dataFile.add("a");
        assertTrue(dataFile.length() > LogHeader.BYTES);
    }

    @Test
    public void position_is_restored_after_reopening() {
        dataFile.add("a");
        dataFile.add("b");

        long pos = dataFile.length();
        dataFile.close();
        dataFile = DataFile.of(Serializers.STRING).open(testFile);

        assertEquals(pos, dataFile.add("c"));
    }

    @Test
    public void iterator_returns_all_data() {
        dataFile.add("a");
        dataFile.add("b");

        List<String> found = Iterators.stream(dataFile.iterator(Direction.FORWARD)).collect(Collectors.toList());
        assertEquals(2, found.size());
        assertEquals("a", found.get(0));
        assertEquals("b", found.get(1));
    }
}