package io.joshworks.fstore.tcp.conduits;

import org.xnio.Buffers;
import org.xnio.Pooled;
import org.xnio.conduits.AbstractSinkConduit;
import org.xnio.conduits.Conduits;
import org.xnio.conduits.MessageSinkConduit;
import org.xnio.conduits.StreamSinkConduit;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.xnio._private.Messages.msg;

public class FramingMessageSinkConduit extends AbstractSinkConduit<StreamSinkConduit> implements MessageSinkConduit {

    public static final int LENGTH_LENGTH = Integer.BYTES;
    private final Pooled<ByteBuffer> transmitBuffer;

    /**
     * Construct a new instance.
     *
     * @param next           the delegate conduit to set
     * @param transmitBuffer the transmit buffer to use
     */
    public FramingMessageSinkConduit(final StreamSinkConduit next, final Pooled<ByteBuffer> transmitBuffer) {
        super(next);
        this.transmitBuffer = transmitBuffer;
    }

    public boolean send(final ByteBuffer src) throws IOException {
        if (!src.hasRemaining()) {
            // no zero messages
            return false;
        }
        final ByteBuffer transmitBuffer = this.transmitBuffer.getResource();
        final int remaining = src.remaining();
        if (remaining > transmitBuffer.capacity() - LENGTH_LENGTH) {
            throw msg.txMsgTooLarge();
        }
        if (transmitBuffer.remaining() < LENGTH_LENGTH + remaining && !writeBuffer()) {
            return false;
        }
        transmitBuffer.putInt(remaining);
        transmitBuffer.put(src);
        writeBuffer();
        return true;
    }

    public boolean send(final ByteBuffer[] srcs, final int offs, final int len) throws IOException {
        if (len == 1) {
            return send(srcs[offs]);
        } else if (!Buffers.hasRemaining(srcs, offs, len)) {
            return false;
        }
        final ByteBuffer transmitBuffer = this.transmitBuffer.getResource();
        final long remaining = Buffers.remaining(srcs, offs, len);
        if (remaining > transmitBuffer.capacity() - LENGTH_LENGTH) {
            throw msg.txMsgTooLarge();
        }
        if (transmitBuffer.remaining() < LENGTH_LENGTH + remaining && !writeBuffer()) {
            return false;
        }
        transmitBuffer.putInt((int) remaining);
        io.joshworks.fstore.core.io.buffers.Buffers.copy(transmitBuffer, srcs, offs, len);
        writeBuffer();
        return true;
    }

    @Override
    public boolean sendFinal(ByteBuffer src) throws IOException {
        //TODO: non-naive implementation
        return Conduits.sendFinalBasic(this, src);
    }

    @Override
    public boolean sendFinal(ByteBuffer[] srcs, int offs, int len) throws IOException {
        return Conduits.sendFinalBasic(this, srcs, offs, len);
    }

    private boolean writeBuffer() throws IOException {
        final ByteBuffer buffer = transmitBuffer.getResource();
        if (buffer.position() > 0) buffer.flip();
        try {
            while (buffer.hasRemaining()) {
                final int res = next.write(buffer);
                if (res == 0) {
                    return false;
                }
            }
            return true;
        } finally {
            buffer.compact();
        }
    }

    public boolean flush() throws IOException {
        return writeBuffer() && next.flush();
    }

    public void terminateWrites() throws IOException {
        transmitBuffer.free();
        next.terminateWrites();
    }

    public void truncateWrites() throws IOException {
        transmitBuffer.free();
        next.truncateWrites();
    }
}
