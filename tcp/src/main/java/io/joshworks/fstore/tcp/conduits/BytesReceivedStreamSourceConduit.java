package io.joshworks.fstore.tcp.conduits;

import org.xnio.channels.StreamSinkChannel;
import org.xnio.conduits.AbstractStreamSourceConduit;
import org.xnio.conduits.StreamSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.LongConsumer;

public class BytesReceivedStreamSourceConduit extends AbstractStreamSourceConduit<StreamSourceConduit> {

    private final LongConsumer callback;

    public BytesReceivedStreamSourceConduit(StreamSourceConduit next, LongConsumer callback) {
        super(next);
        this.callback = callback;
    }

    @Override
    public void suspendReads() {
        super.suspendReads();
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
            callback.accept(i);
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
