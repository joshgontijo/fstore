package io.joshworks.fstore.stream;

import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamsTest {

    private static final int STREAMS_FLUSH_THRESHOLD = 100000;
    private Streams streams;

    private File dummyFile;

    @Before
    public void setUp() {
        dummyFile = TestUtils.testFolder();
        streams = open();
    }

    private Streams open() {
        return new Streams(dummyFile, STREAMS_FLUSH_THRESHOLD, Cache.softCache());
    }

    @After
    public void tearDown() {
        streams.close();
        TestUtils.deleteRecursively(dummyFile);
    }


    @Test(expected = StreamException.class)
    public void stream_with_same_name_is_not_created() {
        streams.create("a");
        streams.create("a");
    }

    @Test
    public void get_returns_correct_stream() {
        StreamMetadata created = streams.create("a", 0, 1);
        assertNotNull(streams.get(created.hash));
    }

    @Test
    public void streamsStartingWith() {

        streams.create("abc-123", 0, 1);
        streams.create("abc-345", 0, 2);
        streams.create("another1", 0, 3);
        streams.create("another2", 0, 4);

        Set<String> names = streams.matchStreamName(Set.of("abc-*"));

        assertEquals(2, names.size());
        assertTrue(names.contains("abc-123"));
        assertTrue(names.contains("abc-345"));
    }

    @Test
    public void streamsEndingWith() {

        streams.create("44444aaa", 0, 1);
        streams.create("123-aaa", 0, 2);
        streams.create("another1", 0, 3);
        streams.create("another2", 0, 4);

        Set<String> names = streams.matchStreamName(Set.of("*aaa"));

        assertEquals(2, names.size());
        assertTrue(names.contains("44444aaa"));
        assertTrue(names.contains("123-aaa"));
    }

    @Test
    public void streamsContaining() {

        streams.create("aaaayolobbb", 0, 1);
        streams.create("123-yolo", 0, 2);
        streams.create("another1", 0, 3);
        streams.create("another2", 0, 4);

        Set<String> names = streams.matchStreamName(Set.of("*yolo*"));

        assertEquals(2, names.size());
        assertTrue(names.contains("aaaayolobbb"));
        assertTrue(names.contains("123-yolo"));
    }

    @Test
    public void streams_are_loaded_after_restarting_WITH_DISK_ITEMS() {

        int numStreams = STREAMS_FLUSH_THRESHOLD + 10; //2 segments + 10 memItems
        for (int i = 0; i < numStreams; i++) {
            streams.create(String.valueOf(i));
        }

        streams.close();
        streams = open();

        for (int i = 0; i < numStreams; i++) {
            StreamMetadata streamInfo = streams.get(String.valueOf(i));
            assertNotNull("Failed on " + i, streamInfo);
        }
    }

    @Test
    public void streams_are_loaded_after_restarting_WITH_MEM_ONLY_ITEMS() {

        int numStreams = STREAMS_FLUSH_THRESHOLD - 1;
        for (int i = 0; i < numStreams; i++) {
            streams.create(String.valueOf(i));
        }

        streams.close();
        streams = open();

        for (int i = 0; i < numStreams; i++) {
            StreamMetadata streamInfo = streams.get(String.valueOf(i));
            assertNotNull("Failed on " + i, streamInfo);
        }
    }
}