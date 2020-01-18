package io.joshworks.fstore.tcp.conduits;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.buffers.StupidPool;
import io.joshworks.fstore.tcp.codec.CodecRegistry;
import io.joshworks.fstore.tcp.codec.Compression;
import io.joshworks.fstore.tcp.codec.TcpHeader;
import org.xnio.conduits.AbstractSourceConduit;
import org.xnio.conduits.MessageSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;


public class CodecConduit extends AbstractSourceConduit<MessageSourceConduit> implements MessageSourceConduit {

    private final StupidPool pool;

    public CodecConduit(MessageSourceConduit source, StupidPool pool) {
        super(source);
        this.pool = pool;
    }

    @Override
    public int receive(ByteBuffer dst) throws IOException {
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

            int ucpLen = TcpHeader.uncompressedLength(compressed);
            Compression compression = TcpHeader.compression(compressed);
            Buffers.offsetPosition(compressed, TcpHeader.BYTES);

            if (Compression.NONE.equals(compression)) {
                return Buffers.copy(compressed, dst);
            }

            if (ucpLen > dst.remaining()) {
                throw new IllegalStateException("Cannot decompress: Uncompressed length: " + ucpLen + ", target buffer: " + dst.remaining());
            }

            Codec codec = CodecRegistry.lookup(compression);
            codec.decompress(compressed, dst);
            return ucpLen;

        } finally {
            pool.free(compressed);
        }
    }

    @Override
    public long receive(ByteBuffer[] dsts, int offs, int len) throws IOException {
        ByteBuffer compressed = pool.allocate();
        try {
            int received = next.receive(compressed);
            compressed.flip();

            Compression compression = TcpHeader.compression(compressed);
            Codec codec = CodecRegistry.lookup(compression);
            if (Compression.NONE.equals(compression)) {
                return Buffers.copy(dsts, offs, len, compressed);
            }

            int ucpLen = TcpHeader.uncompressedLength(compressed);
            ByteBuffer tmp = Buffers.allocate(ucpLen, compressed.isDirect());

            codec.decompress(compressed, tmp);
            tmp.flip();
            return Buffers.copy(dsts, offs, len, tmp);
        } finally {
            pool.free(compressed);
        }


    }
}
