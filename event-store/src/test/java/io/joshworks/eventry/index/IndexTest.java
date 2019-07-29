package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;

public class IndexTest {

    private Index index;
    private File testDir;
    private static final int FLUSH_THRESHOLD = 100;

    @Before
    public void setUp() {
        testDir = FileUtils.testFolder();
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

        IndexIterator iterator = index.iterator(Checkpoint.of(Set.of(1L, 2L)));
        while(iterator.hasNext()) {
            IndexEntry next = iterator.next();
            System.out.println(next);
        }

    }

    private Index index(Function<Long, StreamMetadata> metadataSupplier) {
        return new Index(testDir, FLUSH_THRESHOLD, Cache.softCache(), metadataSupplier);
    }

}