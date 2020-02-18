package io.joshworks.fstore.tcp.conduits;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import org.xnio.Buffers;
import org.xnio.conduits.AbstractSinkConduit;
import org.xnio.conduits.Conduits;
import org.xnio.conduits.MessageSinkConduit;
import org.xnio.conduits.StreamSinkConduit;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.joshworks.fstore.tcp.TcpHeader.RECORD_LEN_LENGTH;
import static org.xnio._private.Messages.msg;

public class FramingMessageSinkConduit extends AbstractSinkConduit<StreamSinkConduit> implements MessageSinkConduit {

    private final ByteBuffer transmitBuffer;
    private final BufferPool pool;


    public FramingMessageSinkConduit(final StreamSinkConduit next, BufferPool pool) {
        super(next);
        this.pool = pool;
        this.transmitBuffer = pool.allocate();
    }

    public boolean send(final ByteBuffer src) throws IOException {
        if (!src.hasRemaining()) {
            // no zero messages
            return false;
        }
        final int remaining = src.remaining();
        if (remaining > transmitBuffer.capacity() - RECORD_LEN_LENGTH) {
            throw msg.txMsgTooLarge();
        }
        if (transmitBuffer.remaining() < RECORD_LEN_LENGTH + remaining && !writeBuffer()) {
            return false;
        }
        transmitBuffer.putInt(remaining);
        transmitBuffer.put(src);
        writeBuffer();
        return true;
    }

    @Override
    public boolean send(final ByteBuffer[] srcs, final int offs, final int len) throws IOException {
        if (len == 1) {
            return send(srcs[offs]);
        } else if (!Buffers.hasRemaining(srcs, offs, len)) {
            return false;
        }
        final long remaining = Buffers.remaining(srcs, offs, len);
        if (remaining > transmitBuffer.capacity() - RECORD_LEN_LENGTH) {
            throw msg.txMsgTooLarge();
        }
        if (transmitBuffer.remaining() < RECORD_LEN_LENGTH + remaining && !writeBuffer()) {
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
        if (transmitBuffer.position() > 0) {
            transmitBuffer.flip();
        }
        try {
            while (transmitBuffer.hasRemaining()) {
                final int res = next.write(transmitBuffer);
                if (res == 0) {
                    return false;
                }
            }
            return true;
        } finally {
            transmitBuffer.compact();
        }
    }

    @Override
    public boolean flush() throws IOException {
        //ONLY FLUSH IF THERE'S DATA TO BE FLUSHED, OTHERWISE WE WILL FLUSH GARBAGE
        if (transmitBuffer.position() > 0) {
            writeBuffer();
        }
        return next.flush();
    }

    @Override
    public void terminateWrites() throws IOException {
        pool.free(transmitBuffer);
        next.terminateWrites();
    }

    @Override
    public void truncateWrites() throws IOException {
        pool.free(transmitBuffer);
        next.truncateWrites();
    }
}
