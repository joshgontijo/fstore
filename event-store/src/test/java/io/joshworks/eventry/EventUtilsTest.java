package io.joshworks.eventry;

import io.joshworks.eventry.stream.StreamMetadata;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EventUtilsTest {

    @Test
    public void truncated_version_equals_to_truncated() {
        int truncated = 10;
        int version = truncated;
        var metadata = new StreamMetadata("a", 123, 0, NO_MAX_AGE, NO_MAX_COUNT, truncated, Map.of(), Map.of(), 0);
        boolean valid = EventUtils.validIndexEntry(metadata, version, 0, l -> version);
        assertFalse(valid);
    }

    @Test
    public void truncated_version_less_than_truncated() {
        int truncated = 10;
        int version = truncated - 1;
        var metadata = new StreamMetadata("a", 123, 0, NO_MAX_AGE, NO_MAX_COUNT, truncated, Map.of(), Map.of(), 0);
        boolean valid = EventUtils.validIndexEntry(metadata, version, 0, l -> version);
        assertFalse(valid);
    }

    @Test
    public void truncated_version_greater_than_truncated() {
        int truncated = 10;
        int version = truncated + 1;
        var metadata = new StreamMetadata("a", 123, 0, NO_MAX_AGE, NO_MAX_COUNT, truncated, Map.of(), Map.of(), 0);
        boolean valid = EventUtils.validIndexEntry(metadata, version, 0, l -> version);
        assertTrue(valid);
    }

    @Test
    public void max_aged() {
        long now = Instant.now().getEpochSecond();
        long old = now - 2;
        long maxAge = now - 1;
        var metadata = new StreamMetadata("a", 123, 0, maxAge, NO_MAX_COUNT, NO_TRUNCATE, Map.of(), Map.of(), 0);
        boolean valid = EventUtils.validIndexEntry(metadata, 0, old, l -> 0);
        assertFalse(valid);
    }

    @Test
    public void max_count() {
        int version = 1;
        int streamVersion = 10;
        int maxCount = 1;
        var metadata = new StreamMetadata("a", 123, 0, NO_MAX_AGE, maxCount, NO_TRUNCATE, Map.of(), Map.of(), 0);
        boolean valid = EventUtils.validIndexEntry(metadata, version, 0, l -> streamVersion);
        assertFalse(valid);
    }
}