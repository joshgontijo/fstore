package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.nio.ByteBuffer;

class ReadListener implements ChannelListener<ConduitStreamSourceChannel> {

    private static final Logger logger = LoggerFactory.getLogger(ReadListener.class);

    private final TcpConnection tcpConnection;
    private final BufferPool pool;
    private final EventHandler handler;

    ReadListener(TcpConnection tcpConnection, EventHandler handler, BufferPool pool) {
        this.tcpConnection = tcpConnection;
        this.handler = handler;
        this.pool = pool;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        try {
            int read;
            do {
                ByteBuffer buffer = pool.allocate();
                read = channel.read(buffer);
                buffer.flip();
                if (buffer.hasRemaining()) {
                    dispatch(tcpConnection, buffer);
                }
            } while (read > 0);

            if (read == -1) {
                IoUtils.safeClose(channel);
            }

        } catch (Exception e) {
            logger.warn("Error while reading message", e);
            IoUtils.safeClose(channel);
        }
    }

    private void dispatch(TcpConnection tcpConnection, ByteBuffer buffer) {
        try {
            tcpConnection.incrementMessageReceived();
            tcpConnection.worker().execute(() -> handleEvent(tcpConnection, buffer));
        } catch (Exception e) {
            logger.error("Event handler threw an exception", e);
        }
    }

    private void handleEvent(TcpConnection tcpConnection, ByteBuffer buffer) {
        try {
            if (!buffer.hasRemaining()) {
                throw new IllegalStateException("Empty buffer");
            }
            handler.onEvent(tcpConnection, buffer);
        } finally {
            pool.free(buffer);
        }
    }
}
