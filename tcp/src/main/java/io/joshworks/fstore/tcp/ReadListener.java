package io.joshworks.fstore.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.nio.ByteBuffer;

class ReadListener implements ChannelListener<ConduitStreamSourceChannel> {

    private static final Logger logger = LoggerFactory.getLogger(ReadListener.class);

    private final TcpConnection conn;
    private final boolean async;
    private final EventHandler handler;

    ReadListener(TcpConnection conn, EventHandler handler, boolean async) {
        this.conn = conn;
        this.handler = handler;
        this.async = async;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        try {
            int read;
            do {
                ByteBuffer buffer = conn.pool().allocate();
                read = channel.read(buffer);
                buffer.flip();
                if (buffer.hasRemaining()) {
                    dispatch(conn, buffer);
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
            if (async) {
                handleEventAsync(tcpConnection, buffer);
            } else {
                handleEvent(tcpConnection, buffer);
            }
        } catch (Exception e) {
            logger.error("Event handler threw an exception", e);
        }
    }

    private void handleEvent(TcpConnection tcpConnection, ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            throw new IllegalStateException("Empty buffer");
        }
        handler.onEvent(tcpConnection, buffer);
    }

    private void handleEventAsync(TcpConnection tcpConnection, ByteBuffer buffer) {
        tcpConnection.worker().execute(() -> {
            try {
                handleEvent(tcpConnection, buffer);
            } finally {
                conn.pool().free(buffer);
            }
        });

    }
}
