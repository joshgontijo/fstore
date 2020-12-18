package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.EventSerializer;
import io.joshworks.es2.sstable.SSTables;
import io.joshworks.es2.sstable.StreamBlockDeserializer;
import io.joshworks.es2.sstable.TestEvent;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemTableTest {

    private MemTable memTable;

    @Before
    public void setUp() throws Exception {
        memTable = new MemTable(Size.MB.ofInt(2), false);
    }

    @Test
    public void get() {
        String stream = "stream-1";
        long streamHash = StreamHasher.hash(stream);

        ByteBuffer item1 = EventSerializer.serialize(stream, "type-1", 0, "data", 0);
        ByteBuffer item2 = EventSerializer.serialize(stream, "type-1", 1, "data", 0);
        memTable.add(item1);
        memTable.add(item2);

        Sink.Memory sink = new Sink.Memory();
        int res = memTable.get(streamHash, 0, sink);
        assertTrue(res > 0);
    }

    @Test
    public void version() {
        String stream = "stream-1";
        long streamHash = StreamHasher.hash(stream);

        ByteBuffer item1 = EventSerializer.serialize(stream, "type-1", 0, "data", 0);
        ByteBuffer item2 = EventSerializer.serialize(stream, "type-1", 1, "data", 0);
        memTable.add(item1);
        memTable.add(item2);

        assertEquals(1, memTable.version(streamHash));
    }

    @Test
    public void flush() {
        String stream = "stream-1";
        long streamHash = StreamHasher.hash(stream);

        TestEvent item1 = TestEvent.create(stream,  0, 0, "data", "ev-1");
        TestEvent item2 = TestEvent.create(stream,  1, 1, "data", "ev-2");
        memTable.add(item1.serialize());
        memTable.add(item2.serialize());

        SSTables channel = new SSTables(TestUtils.testFolder().toPath());
        try {
            memTable.flush(channel);

            Sink.Memory sink = new Sink.Memory();
            int res = channel.get(streamHash, 0, sink);
            assertTrue(res > 0);

            List<TestEvent> entries = StreamBlockDeserializer.deserialize(sink.data());
            assertEquals(2, entries.size());
            assertEquals(item1, entries.get(0));
            assertEquals(item2, entries.get(1));

        } finally {
            channel.delete();
        }

    }

    @Test
    public void clear() {
    }

    @Test
    public void entries() {
        ByteBuffer item1 = EventSerializer.serialize("s1", "type-1", 0, "data", 0);
        ByteBuffer item2 = EventSerializer.serialize("s2", "type-1", 0, "data", 0);
        memTable.add(item1);
        memTable.add(item2);

        assertEquals(2, memTable.entries());
    }

    @Test
    public void size() {
        memTable.add(EventSerializer.serialize("s1", "type-1", 0, "data", 0));
        assertTrue(memTable.size() > 0);
    }
}