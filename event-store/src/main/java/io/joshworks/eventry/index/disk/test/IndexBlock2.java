package io.joshworks.eventry.index.disk.test;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.BaseBlock;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.EntrySerializer;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Format:
 * [UNCOMPRESSED_SIZE][ENTRY_COUNT]
 * |streamHash entryCount | version1 pos1 | version2 pos2 | versionN posN |
 * <p>
 * Where:
 * streamHash - 64bits
 * entryCount - 32bits
 * version - 32bits
 * pos - 64bits
 * <p>
 * Definitions:
 * streamHash: any real number
 * entryCount: greater than zero
 * pos: greater or equal zero
 * <p>
 * version: positive greater or equal zero for insertions, negative value representing the truncated before version
 * of the it's absolute value, example... -10 represents truncated versions from version 0 until 10.
 * So only version 11 onwards will be available.
 * <p>
 * Deleting a stream means the truncated version will be Integer.MIN_VALUE
 * <p>
 * Entry.deletion field is ignored
 */
public class IndexBlock2 extends BaseBlock {

    private static final Serializer<Entry<IndexKey, Long>> serializer = new EntrySerializer<>(new IndexKeySerializer(), Serializers.LONG);

    private static final int ENTRY_SIZE = IndexKey.BYTES + Long.BYTES;

    public IndexBlock2(int maxSize) {
        super(maxSize);
    }

    protected IndexBlock2(Codec codec, ByteBuffer data) {
        super(codec, data);
    }

    @Override
    public ByteBuffer pack(Codec codec) {
        if (buffers.isEmpty()) {
            return ByteBuffer.allocate(0);
        }

        int maxVersionSizeOverhead = entryCount() * Integer.BYTES;
        int actualSize = ENTRY_SIZE * entryCount();
        var packedBuffer = ByteBuffer.allocate(Integer.BYTES + actualSize + maxVersionSizeOverhead);

        packedBuffer.putInt(entryCount());

        Entry<IndexKey, Long> last = null;
        List<Integer> versions = new ArrayList<>();
        List<Long> positions = new ArrayList<>();
        for (ByteBuffer buffer : buffers) {
            Entry<IndexKey, Long> indexEntry = serializer.fromBytes(buffer.asReadOnlyBuffer());
            if (last == null) {
                last = indexEntry;
            }
            if (last.key.stream != indexEntry.key.stream) {
                writeToBuffer(packedBuffer, last.key.stream, versions, positions);
                versions = new ArrayList<>();
                positions = new ArrayList<>();
            }

            versions.add(indexEntry.key.version);
            positions.add(indexEntry.value);
            last = indexEntry;
        }
        if (last != null && !versions.isEmpty()) {
            writeToBuffer(packedBuffer, last.key.stream, versions, positions);
        }

        packedBuffer.flip();
        return codec.compress(packedBuffer);
    }

    @Override
    protected int unpack(Codec codec, ByteBuffer readBuffer) {
        ByteBuffer decompressed = codec.decompress(readBuffer);
        int entryCount = decompressed.getInt();
        while (decompressed.hasRemaining()) {
            long stream = decompressed.getLong();
            int numVersions = decompressed.getInt();
            for (int i = 0; i < numVersions; i++) {
                int version = decompressed.getInt();
                long position = decompressed.getLong();
                Entry<IndexKey, Long> ie = Entry.of(false, new IndexKey(stream, version), position);
                buffers.add(serializer.toBytes(ie));
            }
        }
        if (buffers.size() != entryCount) {
            throw new IllegalStateException("Expected " + entryCount + " got " + buffers.size());
        }
        return buffers.size() * ENTRY_SIZE;
    }

    private void writeToBuffer(ByteBuffer buffer, long stream, List<Integer> versions, List<Long> positions) {
        buffer.putLong(stream);
        buffer.putInt(versions.size());
        for (int i = 0; i < versions.size(); i++) {
            buffer.putInt(versions.get(i));
            buffer.putLong(positions.get(i));
        }
    }

    public static BlockFactory factory() {
        return new Index2BlockFactory();
    }

    private static class Index2BlockFactory implements BlockFactory {
        @Override
        public Block create(int maxBlockSize) {
            return new IndexBlock2(maxBlockSize);
        }

        @Override
        public Block load(Codec codec, ByteBuffer data) {
            return new IndexBlock2(codec, data);
        }
    }


}
