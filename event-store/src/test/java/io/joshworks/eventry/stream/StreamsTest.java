package io.joshworks.eventry.stream;

import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamsTest {

    private Streams streams;

    private File dummyFile;

    @Before
    public void setUp() {
        dummyFile = FileUtils.testFolder();
        streams = createStreams();
    }

    private Streams createStreams() {
        return new Streams(dummyFile, 10, -1);
    }

    @After
    public void tearDown() {
        streams.close();
        FileUtils.tryDelete(dummyFile);
    }


    @Test(expected = StreamException.class)
    public void stream_with_same_name_is_not_created() {
        streams.create("a");
        streams.create("a");
    }

    @Test
    public void get_returns_correct_stream() {
        StreamMetadata created = streams.create("a", 1, 0);
        assertNotNull(streams.get(created.hash));
    }

    @Test
    public void streamsStartingWith() {

        streams.create("abc-123", 1, 0);
        streams.create("abc-345", 2, 0);
        streams.create("another1", 3, 0);
        streams.create("another2", 4, 0);

        Set<String> names = streams.matchStreamName("abc-*");

        assertEquals(2, names.size());
        assertTrue(names.contains("abc-123"));
        assertTrue(names.contains("abc-345"));
    }

    @Test
    public void streams_are_loaded_after_restarting() {

        int numStreams = 1000001;
        for (int i = 0; i < numStreams; i++) {
            if(20326 == i) {
                System.out.println();
            }
            streams.create(String.valueOf(i));
        }

        streams.close();
        streams = createStreams();

        for (int i = 0; i < numStreams; i++) {
            if(20326 == i) {
                System.out.println();
            }
            StreamMetadata streamInfo = streams.get(String.valueOf(i));
            assertNotNull("Failed on " + i, streamInfo);
        }
    }
}