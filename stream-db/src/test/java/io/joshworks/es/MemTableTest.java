package io.joshworks.es;

import io.joshworks.es.conduit.Sink;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemTableTest {

    private MemTable table;

    @Before
    public void setUp() {
        table = new MemTable(Size.MB.ofInt(1), false);
    }

    @Test
    public void add() {
        String stream = "stream-1";
        ByteBuffer ev1 = EventUtils.create(stream, "abc", 0, "test");
        table.add(ev1);

        ByteBuffer ev2 = EventUtils.create(stream, "abc", 1, "test");
        table.add(ev2);

        BufferSink sink = new BufferSink();
        long streamHash = StreamHasher.hash(stream);
        long bytes = table.get(streamHash, 0, sink);

        assertTrue(bytes > 0);
        assertEquals(2, sink.buffers.size());
    }

    @Test
    public void version() {
        String stream = "stream-1";
        ByteBuffer ev1 = EventUtils.create(stream, "abc", 0, "test");
        table.add(ev1);

        ByteBuffer ev2 = EventUtils.create(stream, "abc", 1, "test");
        table.add(ev2);

        BufferSink sink = new BufferSink();
        long streamHash = StreamHasher.hash(stream);


        assertEquals(1, table.version(streamHash));
    }

    @Test
    public void flush() {
        String stream = "stream-1";
        ByteBuffer ev1 = EventUtils.create(stream, "abc", 0, "test");
        table.add(ev1);

        ByteBuffer ev2 = EventUtils.create(stream, "abc", 1, "test");
        table.add(ev2);

        BufferSink sink = new BufferSink();
        long bytes = table.flush(sink);

        assertTrue(bytes > 0);
        assertEquals(2, sink.buffers.size());
    }

    @Test
    public void clear() {
        String stream = "stream-1";
        ByteBuffer ev1 = EventUtils.create(stream, "abc", 0, "test");
        table.add(ev1);

        assertEquals(1, table.entries());
        assertTrue(table.size() > 0);

        table.clear();

        assertEquals(0, table.entries());
        assertEquals(0, table.size());
    }

    private static class BufferSink implements Sink {

        private final List<ByteBuffer> buffers = new ArrayList<>();

        @Override
        public int write(ByteBuffer src) {
            ByteBuffer dst = Buffers.allocate(src.remaining(), false);
            dst.put(src);
            buffers.add(dst);
            dst.flip();
            return dst.remaining();
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {

        }
    }

}