package io.joshworks.fstore.index;

import io.joshworks.fstore.stream.StreamMetadata;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.joshworks.fstore.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.fstore.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.fstore.stream.StreamMetadata.NO_TRUNCATE;
import static org.junit.Assert.assertEquals;

public class IndexTest {

    private Index index;
    private File testDir;
    private static final int FLUSH_THRESHOLD = 100;

    @Before
    public void setUp() {
        testDir = TestUtils.testFolder();
        index = new Index(testDir, FLUSH_THRESHOLD, Cache.softCache(), stream -> {
            return new StreamMetadata(String.valueOf(stream), stream, 0, NO_MAX_AGE, NO_MAX_COUNT, NO_TRUNCATE, new HashMap<>(), new HashMap<>(), StreamMetadata.STREAM_ACTIVE);
        });
    }

    private Map.Entry<Long, StreamMetadata> metadata(long stream, int maxAgeSec, int maxCount, int truncatedVersion) {
        StreamMetadata metadata = new StreamMetadata(String.valueOf(stream), stream, 0, maxAgeSec, maxCount, truncatedVersion, new HashMap<>(), new HashMap<>(), StreamMetadata.STREAM_ACTIVE);
        return new AbstractMap.SimpleEntry<>(stream, metadata);
    }

    @Test
    public void truncated_entries_are_removed_on_compaction() {
        var metadataItems = Map.ofEntries(
                metadata(1L, NO_MAX_AGE, NO_MAX_COUNT, 50),
                metadata(2L, NO_MAX_AGE, NO_MAX_COUNT, NO_TRUNCATE)
        );

        index = index(metadataItems::get);

        for (int i = 0; i < FLUSH_THRESHOLD * 5; i++) {
            index.add(1L, i, i);
            index.add(2L, i, i);
        }
        index.compact();

        IndexIterator iterator = index.iterator(Direction.FORWARD, EventMap.of(Set.of(1L, 2L)));
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            System.out.println(next);
        }
    }

    @Test
    public void backward_read() {
        long stream = 123L;
        int items = FLUSH_THRESHOLD * 5;
        for (int i = 0; i < FLUSH_THRESHOLD * 5; i++) {
            index.add(stream, i, i);
        }

        long count = Iterators.closeableStream(index.iterator(Direction.BACKWARD, EventMap.of(stream, items))).count();
        assertEquals(items, count);

        IndexIterator iterator = index.iterator(Direction.BACKWARD, EventMap.of(stream, items));
        int expectedVersion = items;
        while (iterator.hasNext()) {
            IndexEntry ie = iterator.next();
            assertEquals(expectedVersion - 1, ie.version);
            expectedVersion = ie.version;
        }
    }

    private Index index(Function<Long, StreamMetadata> metadataSupplier) {
        return new Index(testDir, FLUSH_THRESHOLD, Cache.softCache(), metadataSupplier);
    }

}