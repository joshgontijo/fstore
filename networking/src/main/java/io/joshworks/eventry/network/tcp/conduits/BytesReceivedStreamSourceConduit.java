package io.joshworks.eventry.network.tcp.conduits;

import org.xnio.channels.StreamSinkChannel;
import org.xnio.conduits.AbstractStreamSourceConduit;
import org.xnio.conduits.StreamSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Consumer;

public class BytesReceivedStreamSourceConduit extends AbstractStreamSourceConduit<StreamSourceConduit> {

    private final Consumer<Long> callback;

    /**
     * Construct a new instance.
     *
     * @param next the delegate conduit to set
     * @param callback
     */
    public BytesReceivedStreamSourceConduit(StreamSourceConduit next, Consumer<Long> callback) {
        super(next);
        this.callback = callback;
    }

    @Override
    public long transferTo(long position, long count, FileChannel target) throws IOException {
        long l = super.transferTo(position, count, target);
        if (l > 0) {
            callback.accept(l);
        }
        return l;
    }

    @Override
    public long transferTo(long count, ByteBuffer throughBuffer, StreamSinkChannel target) throws IOException {
        long l = super.transferTo(count, throughBuffer, target);
        if (l > 0) {
            callback.accept(l);
        }
        return l;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int i = super.read(dst);
        if (i > 0) {
            callback.accept((long) i);
        }
        return i;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offs, int len) throws IOException {
        long l = super.read(dsts, offs, len);
        if (l > 0) {
            callback.accept(l);
        }
        return l;
    }
}
