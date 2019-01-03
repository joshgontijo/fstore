package io.joshworks.eventry.stream;

import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamsTest {

    private Streams streams;

    private File dummyFile;

    @Before
    public void setUp() {
        dummyFile = FileUtils.testFolder();
        streams = new Streams(dummyFile, 10, streamHash -> -1);
    }

    @After
    public void tearDown() {
        streams.close();
        FileUtils.tryDelete(dummyFile);
    }


    @Test(expected = IllegalArgumentException.class)
    public void stream_with_same_name_is_not_created() {
        streams.create("a");
        streams.create("a");
    }

    @Test
    public void get_returns_correct_stream() {
        StreamMetadata created = streams.create("a", 1, 0);
        assertTrue(streams.get(created.hash).isPresent());
    }

    @Test
    public void streamsStartingWith() {

        streams.create("abc-123", 1, 0);
        streams.create("abc-345", 2, 0);
        streams.create("another1", 3, 0);
        streams.create("another2", 4, 0);

        Set<String> names = streams.streamMatching("abc-*");

        assertEquals(2, names.size());
        assertTrue(names.contains("abc-123"));
        assertTrue(names.contains("abc-345"));
    }

    @Test
    public void version_of_nonExisting_stream_returns_zero() {
        int version = streams.tryIncrementVersion(123, -1);
        assertEquals(0, version);
    }

    @Test(expected = IllegalArgumentException.class)
    public void unexpected_version_throws_exception() {
        streams.tryIncrementVersion(123, 1);
        fail("Expected version mismatch");
    }

    @Test
    public void existing_stream_returns_correct_version() {
        streams.tryIncrementVersion(123, -1);
        int version1 = streams.tryIncrementVersion(123, 0);
        assertEquals(1, version1);

        int version2 = streams.tryIncrementVersion(123, 1);
        assertEquals(2, version2);
    }

    @Test
    public void streams_are_loaded_after_restarting() {

        int numStreams = 1000;
        for (int i = 0; i < numStreams; i++) {
            streams.create(String.valueOf(i));
        }

        streams.close();
        streams = new Streams(dummyFile, 10, streamHash -> -1);

        for (int i = 0; i < numStreams; i++) {
            Optional<StreamMetadata> streamInfo = streams.get(String.valueOf(i));
            assertTrue(streamInfo.isPresent());
        }
    }

    @Test
    public void truncate_sets_truncateBefore_field() {
        String stream = "stream-123";
        int tbVersion = 0;
        StreamMetadata metadata = streams.create(stream);
        streams.tryIncrementVersion(metadata.hash, NO_VERSION); //0
        streams.tryIncrementVersion(metadata.hash, NO_VERSION); //1

        streams.truncate(stream, tbVersion);

        StreamMetadata found = streams.get(stream).get();
        assertEquals(found.truncateBefore, tbVersion);
    }
}