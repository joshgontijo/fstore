package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.EventSerializer;
import io.joshworks.fstore.core.util.Size;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

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