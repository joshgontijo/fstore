package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.BaseBlock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Format:
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
 */
public class IndexBlock extends BaseBlock {

    private static final Serializer<IndexEntry> serializer = new IndexEntrySerializer();

    public IndexBlock(int maxSize) {
        super(maxSize);
    }

    protected IndexBlock(Codec codec, ByteBuffer data) {
        super(codec, data);
    }

    @Override
    public ByteBuffer pack(Codec codec) {
        if (buffers.isEmpty()) {
            return ByteBuffer.allocate(0);
        }
        int maxVersionSizeOverhead = entryCount() * Integer.BYTES;
        int actualSize = IndexEntry.BYTES * entryCount();
        var packedBuffer = ByteBuffer.allocate(actualSize + maxVersionSizeOverhead);

        IndexEntry last = null;
        List<Integer> versions = new ArrayList<>();
        List<Long> positions = new ArrayList<>();
        for (ByteBuffer buffer : buffers) {
            IndexEntry indexEntry = serializer.fromBytes(buffer.asReadOnlyBuffer());
            if (last == null) {
                last = indexEntry;
            }
            if (last.stream != indexEntry.stream) {
                writeToBuffer(packedBuffer, last.stream, versions, positions);
                versions = new ArrayList<>();
                positions = new ArrayList<>();
            }

            versions.add(indexEntry.version);
            positions.add(indexEntry.position);
            last = indexEntry;
        }
        if (last != null && !versions.isEmpty()) {
            writeToBuffer(packedBuffer, last.stream, versions, positions);
        }

        packedBuffer.flip();
        return codec.compress(packedBuffer);
    }

    @Override
    protected int unpack(Codec codec, ByteBuffer readBuffer) {
        ByteBuffer decompressed = codec.decompress(readBuffer);
        while (decompressed.hasRemaining()) {
            long stream = decompressed.getLong();
            int numVersions = decompressed.getInt();
            for (int i = 0; i < numVersions; i++) {
                int version = decompressed.getInt();
                long position = decompressed.getLong();
                IndexEntry ie = IndexEntry.of(stream, version, position);
                buffers.add(serializer.toBytes(ie));
            }
        }
        return buffers.size() * IndexEntry.BYTES;
    }

    private void writeToBuffer(ByteBuffer buffer, long stream, List<Integer> versions, List<Long> positions) {
        buffer.putLong(stream);
        buffer.putInt(versions.size());
        for (int i = 0; i < versions.size(); i++) {
            buffer.putInt(versions.get(i));
            buffer.putLong(positions.get(i));
        }
    }

}
