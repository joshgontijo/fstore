package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Common block test, entries must have the same length in order to accommodate FLenBlock
 */
public abstract class BlockTest {

    private static final int BLOCK_SIZE = Memory.PAGE_SIZE;
    private static final boolean DIRECT = false;
    private BlockFactory factory;
    private Codec codec = Codec.noCompression();

    @Before
    public void setUp() {
        factory = factory(DIRECT);
    }

    public abstract BlockFactory factory(boolean direct);

    @Test(expected = IllegalArgumentException.class)
    public void writing_entry_bigger_than_block_throws_exception() {
        Block block = factory.create(BLOCK_SIZE);

        StringBuilder data = new StringBuilder();
        for (int i = 0; i < BLOCK_SIZE + 1; i++) {
            data.append("a");
        }
        block.add(write(data.toString()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void writing_empty_entry_throws_exception() {
        Block block = factory.create(BLOCK_SIZE);
        block.add(ByteBuffer.allocate(0));
    }

    @Test(expected = IllegalStateException.class)
    public void packing_a_readOnly_block_throws_exception() {
        Block block = factory.create(BLOCK_SIZE);
        block.add(write("a"));
        ByteBuffer dst = packBlock(block);
        Block loaded = factory.load(codec, dst);

        loaded.pack(codec, dst);
    }

    @Test
    public void packing_an_empty_block_does_not_change_destination_buffer() {
        Block block = factory.create(BLOCK_SIZE);
        ByteBuffer dst = ByteBuffer.allocate(BLOCK_SIZE);
        block.pack(codec, dst);

        assertEquals(0, dst.position());
    }

    @Test
    public void entryCount_is_the_same_after_unpacking() {
        Block block = factory.create(BLOCK_SIZE);
        block.add(write("a"));
        block.add(write("b"));
        block.add(write("c"));

        int entryCount = block.entryCount();
        assertEquals(3, entryCount);

        ByteBuffer dst = packBlock(block);
        Block loaded = factory.load(codec, dst);

        assertEquals(entryCount, loaded.entryCount());
    }

    @Test
    public void entries_is_the_same_after_unpacking() {
        Block block = factory.create(BLOCK_SIZE);
        block.add(write("a"));
        block.add(write("b"));
        block.add(write("c"));

        ByteBuffer dst = packBlock(block);

        Block loaded = factory.load(codec, dst);

        assertEquals("a", read(loaded.get(0)));
        assertEquals("b", read(loaded.get(1)));
        assertEquals("c", read(loaded.get(2)));
    }

    @Test
    public void isEmpty_is_the_same_after_unpacking() {
        Block block = factory.create(BLOCK_SIZE);
        assertTrue(block.isEmpty());
        block.add(write("a"));

        assertFalse(block.isEmpty());

        ByteBuffer dst = packBlock(block);

        Block loaded = factory.load(codec, dst);

        assertFalse(loaded.isEmpty());
    }

    @Test
    public void uncompressedSize_is_the_same_after_unpacking() {
        Block block = factory.create(BLOCK_SIZE);
        block.add(write("a"));
        block.add(write("b"));

        int uncompressedSize = block.uncompressedSize();

        ByteBuffer dst = packBlock(block);
        Block loaded = factory.load(codec, dst);

        assertEquals(uncompressedSize, loaded.uncompressedSize());
    }

    @Test
    public void unpackedBlock_is_block_readOnly() {
        Block block = factory.create(BLOCK_SIZE);
        block.add(write("a"));
        block.add(write("b"));

        ByteBuffer dst = packBlock(block);
        Block loaded = factory.load(codec, dst);
        assertTrue(loaded.readOnly());
    }

    @Test
    public void block_isEmpty() {
        Block block = factory.create(BLOCK_SIZE);
        assertTrue(block.isEmpty());
        block.add(write("a"));
        assertFalse(block.isEmpty());

        ByteBuffer dst = packBlock(block);
        Block loaded = factory.load(codec, dst);
        assertFalse(loaded.isEmpty());
    }

    @Test
    public void clear_reset_block_content() {
        Block block = factory.create(BLOCK_SIZE);
        int emptyUncompressedSize = block.uncompressedSize();
        block.add(write("a"));
        assertFalse(block.isEmpty());
        assertEquals(1, block.entryCount());
        assertTrue(block.uncompressedSize() > emptyUncompressedSize);

        block.clear();

        assertTrue(block.isEmpty());
        assertEquals(0, block.entryCount());
        assertEquals(block.uncompressedSize(), emptyUncompressedSize);
    }

    @Test
    public void iterator_after_unpacking_returns_all_entries() {
        Block block = factory.create(BLOCK_SIZE);
        block.add(write("a"));
        block.add(write("b"));

        ByteBuffer dst = packBlock(block);
        Block loaded = factory.load(codec, dst);

        List<String> entries = new ArrayList<>();
        for (ByteBuffer byteBuffer : loaded) {
            entries.add(read(byteBuffer));
        }

        assertEquals(2, entries.size());
        assertEquals("a", entries.get(0));
        assertEquals("b", entries.get(1));
    }

    @Test
    public void can_get_all_entries_before_unpacking() {
        Block block = factory.create(BLOCK_SIZE);
        block.add(write("a"));
        block.add(write("b"));

        assertEquals("a", read(block, 0));
        assertEquals("b", read(block, 1));
    }


    private ByteBuffer packBlock(Block block) {
        ByteBuffer dst = ByteBuffer.allocate(BLOCK_SIZE);
        block.pack(codec, dst);
        dst.flip();
        return dst;
    }

    private static ByteBuffer write(String data) {
        ByteBuffer bb = ByteBuffer.allocate(data.length());
        Serializers.STRING.writeTo(data, bb);
        return bb.flip();
    }

    private static String read(ByteBuffer data) {
        return Serializers.STRING.fromBytes(data);
    }

    private static String read(Block block, int idx) {
        return Serializers.STRING.fromBytes(block.get(idx));
    }

    public static class DefaultBlockTest extends BlockTest {

        @Override
        public BlockFactory factory(boolean direct) {
            return Block.vlenBlock(direct);
        }
    }

    public static class FixedBlockTest extends BlockTest {

        @Override
        public BlockFactory factory(boolean direct) {
            return Block.flenBlock(direct, 1);
        }

        @Test(expected = IllegalArgumentException.class)
        public void writing_entry_bigger_entry_size_throws_exception() {
            int entrySize = 10;
            Block block = Block.flenBlock(false, entrySize).create(entrySize * 2);

            StringBuilder data = new StringBuilder();
            for (int i = 0; i < entrySize + 1; i++) {
                data.append("a");
            }
            block.add(write(data.toString()));
        }

    }

}