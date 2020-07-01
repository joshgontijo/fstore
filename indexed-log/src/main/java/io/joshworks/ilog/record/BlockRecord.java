package io.joshworks.ilog.record;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.RowKey;

import java.nio.ByteBuffer;

/**
 * -------- HEADER ---------
 * UNCOMPRESSED_SIZE (4bytes)
 * COMPRESSED_SIZE (4bytes)
 * ENTRY_COUNT (4bytes)
 * <p>
 * ------- KEYS REGION -----
 * KEY_ENTRY [OFFSET,KEY]
 * ...
 * -------- COMPRESSED VALUES REGION --------
 * COMPRESSED_BLOCK
 * ...
 */
public class BlockRecord extends Records {

    private static final int HEADER_BYTES = Integer.BYTES * 3;
    //relative to block start
    private static final int UNCOMPRESSED_SIZE_OFFSET = 0;
    private static final int COMPRESSED_SIZE_OFFSET = Integer.BYTES;
    private static final int ENTRY_COUNT_OFFSET = COMPRESSED_SIZE_OFFSET + Integer.BYTES;


    private final Codec codec;
    private final int maxSize;

    BlockRecord(String cachePoolName, RowKey rowKey, int maxItems, StripedBufferPool pool, Codec codec, int maxSize) {
        super(cachePoolName, rowKey, maxItems, pool);
        this.codec = codec;
        this.maxSize = maxSize;
    }

    @Override
    public int read(ByteBuffer data) {
        if (!Record.isValid(data)) {
            throw new RuntimeException("Invalid record");
        }
        int recSize = data.getInt(data.position());
        int blockStart = data.getInt(data.position() + Record2.KEY_OFFSET + rowKey.keySize());
        int blockSize = data.getInt(data.position() + Record2.VALUE_LEN_OFFSET);

        int uncompressedSize = data.getInt(blockStart + UNCOMPRESSED_SIZE_OFFSET);
        int compressedSize = data.getInt(blockStart + COMPRESSED_SIZE_OFFSET);
        int entries = data.getInt(blockStart + ENTRY_COUNT_OFFSET);

        ByteBuffer uncompressed = pool.allocate(uncompressedSize);


        int ppos = data.position();
        int plim = data.limit();
        data.limit(ppos + blockSize).position(blockStart);

        codec.decompress(data, uncompressed);
        uncompressed.flip();
        //TODO add validation

        data.limit(plim).position(ppos);



    }
}
