package io.joshworks.eventry.index;

import io.joshworks.fstore.codec.snappy.Lz4Codec;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.EntrySerializer;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class IndexBlockTest {

    private static final Serializer<Entry<IndexKey, Long>> serializer = new EntrySerializer<>(new IndexKeySerializer(), Serializers.LONG);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Codec> data() {
        return Arrays.asList(Codec.noCompression(), new SnappyCodec(), new Lz4Codec(), new Lz4Codec(true));
    }

    private Codec codec;

    public IndexBlockTest(Codec codec) {
        this.codec = codec;
    }

    @Test
    public void packing_empty_block_returns_empty_buffer() {
        var block = new IndexBlock(4096);
        var packed = block.pack(codec);
        assertEquals(0, packed.limit());
    }

    @Test
    public void packed_buffer_is_ready_to_read_from() {
        var block = new IndexBlock(4096);
        addTo(block, Entry.add(new IndexKey(1, 1), 1L));
        var packed = block.pack(codec);
        assertTrue(packed.remaining() > 0);
    }

    @Test
    public void unpacking_has_the_same_entries() {
        var block = new IndexBlock(4096);

        long stream = 123;
        int version = 0;
        long position = 456;

        boolean added;
        do {
            added = addTo(block, Entry.add(new IndexKey(stream, version), position));
        } while (added);


        ByteBuffer packed = block.pack(codec);

        var unpacked = new IndexBlock(codec, packed);

        for (int i = 0; i < block.entryCount(); i++) {
            assertEquals(block.get(i), unpacked.get(i));
        }
    }

    @Test
    public void entryCount_returns_right_count() {

        var block = new IndexBlock(4096);

        int items = 100;
        long stream = 123;
        long position = 456;
        for (int i = 0; i < items; i++) {
            addTo(block, Entry.add(new IndexKey(stream, i), position));
        }

        assertEquals(items, block.entryCount());
    }

    private boolean addTo(Block block, Entry<IndexKey, Long> ie) {
        return block.add(serializer.toBytes(ie));
    }


}