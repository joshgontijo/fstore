package io.joshworks.fstore.tcp.conduits;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.tcp.TcpHeader;
import io.joshworks.fstore.tcp.codec.CodecRegistry;
import io.joshworks.fstore.tcp.codec.Compression;
import org.xnio.conduits.AbstractSourceConduit;
import org.xnio.conduits.MessageSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongConsumer;


public class CodecConduit extends AbstractSourceConduit<MessageSourceConduit> implements MessageSourceConduit {

    private final BufferPool pool;
    private final LongConsumer callback;

    public CodecConduit(MessageSourceConduit source, BufferPool pool, LongConsumer callback) {
        super(source);
        this.pool = pool;
        this.callback = callback;
    }

    @Override
    public int receive(ByteBuffer dst) {
        ByteBuffer compressed = pool.allocate();
        try {
            int recv = next.receive(compressed);
            if (recv == 0) {
                return 0;
            }
            if (recv == -1) {
                return recv;
            }
            compressed.flip();

            Compression compression = TcpHeader.compression(compressed);
            Buffers.offsetPosition(compressed, TcpHeader.COMPRESSION_LENGTH);

            if (Compression.NONE.equals(compression)) {
                return Buffers.copy(compressed, dst);
            }

            Codec codec = CodecRegistry.lookup(compression);
            codec.decompress(compressed, dst);
            int decompressedSize = dst.remaining();
            callback.accept(decompressedSize);
            return decompressedSize;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            pool.free(compressed);
        }
    }

    @Override
    public long receive(ByteBuffer[] dsts, int offs, int len) throws IOException {
        ByteBuffer compressed = pool.allocate();
        try {
            int recv = next.receive(compressed);
            if (recv == 0) {
                return 0;
            }
            if (recv == -1) {
                return recv;
            }
            compressed.flip();

            Compression compression = TcpHeader.compression(compressed.position(), compressed);
            Buffers.offsetPosition(compressed, TcpHeader.COMPRESSION_LENGTH);
            Codec codec = CodecRegistry.lookup(compression);
            if (Compression.NONE.equals(compression)) {
                return Buffers.copy(dsts, offs, len, compressed);
            }

            ByteBuffer tmp = pool.allocate();
            try {
                codec.decompress(compressed, tmp);
                tmp.flip();
                return Buffers.copy(dsts, offs, len, tmp);
            } finally {
                pool.free(tmp);
            }

        } finally {
            pool.free(compressed);
        }


    }
}
