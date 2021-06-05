package io.joshworks.es2.sstable;

import io.joshworks.es2.StreamHasher;
import io.joshworks.es2.sink.Sink;
import io.joshworks.fstore.core.iterators.Iterators;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTablesTest {

    private SSTables sstables;
    private Path folder;

    @Before
    public void open() {
        folder = TestUtils.testFolder().toPath();
        sstables = new SSTables(folder, Executors.newSingleThreadExecutor());
    }

    @After
    public void tearDown() {
        sstables.delete();
        TestUtils.deleteRecursively(folder.toFile());
    }

    @Test
    public void get() {
        String stream = "stream-1";
        long streamHash = StreamHasher.hash(stream);
        ByteBuffer item1 = createEntry(stream, 0);
        ByteBuffer item2 = createEntry(stream, 1);
        sstables.flush(Iterators.of(item1, item2));

        Sink.Memory mem = new Sink.Memory();
        int res = sstables.get(streamHash, 0, mem);
        assertTrue(res > 0);
    }

    @Test
    public void version() {
        String stream = "stream-1";
        long streamHash = StreamHasher.hash(stream);
        ByteBuffer item1 = createEntry(stream, 0);
        ByteBuffer item2 = createEntry(stream, 1);
        sstables.flush(Iterators.of(item1, item2));

        int version = sstables.version(streamHash);
        assertEquals(1, version);
    }

    @Test
    public void compaction() {
        String stream = "stream-1";
        long streamHash = StreamHasher.hash(stream);
        int numSegments = 10;
        int itemsPerSegment = 100;

        for (int seg = 0; seg < numSegments; seg++) {
            var startVersion = seg * itemsPerSegment;
            sstables.flush(IntStream.range(startVersion, startVersion + itemsPerSegment)
                    .mapToObj(i -> createEntry(stream, i))
                    .iterator());
        }

        int expectedVersion = (numSegments * itemsPerSegment) - 1;
        assertStream(streamHash, expectedVersion);
        sstables.compact().join();
        assertStream(streamHash, expectedVersion);
    }


    private void assertStream(long streamHash, int expectedVersion) {
        int version = sstables.version(streamHash);
        assertEquals(expectedVersion, version);
        for (int i = 0; i < 200; i++) {
            int read = sstables.get(streamHash, i, new Sink.Memory());
            assertTrue(read > 0);
        }
    }

    private ByteBuffer createEntry(String stream, int i) {
        return EventSerializer.serialize(stream, "type-1", i, "data", 0);
    }

    private ByteBuffer createEntry(int i) {
        return createEntry("stream-1", i);
    }
}