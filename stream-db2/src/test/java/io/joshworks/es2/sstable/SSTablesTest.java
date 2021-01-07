package io.joshworks.es2.sstable;

import io.joshworks.es2.StreamHasher;
import io.joshworks.es2.sink.Sink;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTablesTest {

    private SSTables sstables;

    @Before
    public void open() {
        sstables = new SSTables(TestUtils.testFolder().toPath(), Executors.newSingleThreadExecutor());
    }

    @After
    public void tearDown() {
        sstables.delete();
    }

    @Test
    public void get() {
        String stream = "stream-1";
        long streamHash = StreamHasher.hash(stream);
        ByteBuffer item1 = EventSerializer.serialize(stream, "type-1", 0, "data", 0);
        ByteBuffer item2 = EventSerializer.serialize(stream, "type-1", 1, "data", 0);
        sstables.flush(Iterators.of(item1, item2));

        Sink.Memory mem = new Sink.Memory();
        int res = sstables.get(streamHash, 0, mem);
        assertTrue(res > 0);
    }

    @Test
    public void version() {
        String stream = "stream-1";
        long streamHash = StreamHasher.hash(stream);
        ByteBuffer item1 = EventSerializer.serialize(stream, "type-1", 0, "data", 0);
        ByteBuffer item2 = EventSerializer.serialize(stream, "type-1", 1, "data", 0);
        sstables.flush(Iterators.of(item1, item2));

        int version = sstables.version(streamHash);
        assertEquals(1, version);
    }
}