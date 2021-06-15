package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.LengthPrefixedChannelIterator;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.joshworks.es2.StreamHasher.hash;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamBlockIteratorTest {


    private File dataFile;
    private SSTable sstable;

    @Before
    public void open() {
        dataFile = TestUtils.testFile();
    }

    @After
    public void tearDown() {
        if (sstable != null) {
            sstable.delete();
        }
    }

    @Test
    public void iterate_all_decompressed() {
        String stream = "stream-1";
        int items = 10_000;

        List<ByteBuffer> events = IntStream.range(0, items)
                .mapToObj(v -> EventSerializer.serialize(stream, "type-1", v, "data", 0))
                .collect(Collectors.toList());

        sstable = SSTable.create(dataFile, events.iterator(), items, new SSTableConfig().lowConfig);

        var it = new StreamBlockIterator(new LengthPrefixedChannelIterator(sstable.channel));

        for (int i = 0; i < items; i++) {
            assertTrue(it.hasNext());
            var data = it.next();

            assertEquals(hash(stream), Event.stream(data));
            assertEquals(i, Event.version(data));
            assertEquals("type-1", Event.eventType(data));
            assertEquals("data", Event.dataString(data));
        }

    }

}