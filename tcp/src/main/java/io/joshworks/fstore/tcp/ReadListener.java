package io.joshworks.fstore.tcp;

import io.joshworks.fstore.tcp.handlers.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.Pool;
import org.xnio.Pooled;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.nio.ByteBuffer;

class ReadListener implements ChannelListener<ConduitStreamSourceChannel> {

    private static final Logger logger = LoggerFactory.getLogger(ReadListener.class);

    private final TcpConnection tcpConnection;
    private final Pool<ByteBuffer> bufferPool;
    private final EventHandler handler;

    ReadListener(TcpConnection tcpConnection, EventHandler handler, Pool<ByteBuffer> bufferPool) {
        this.tcpConnection = tcpConnection;
        this.handler = handler;
        this.bufferPool = bufferPool;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        Pooled<ByteBuffer> polledBuffer = bufferPool.allocate();
        try {
            int read;
            do {
                ByteBuffer buffer = polledBuffer.getResource();
                read = channel.read(buffer);
                buffer.flip();
                if (buffer.hasRemaining()) {
                    dispatch(tcpConnection, polledBuffer);
                }
            } while (read > 0);

            if (read == -1) {
                IoUtils.safeClose(channel);
            }

        } catch (Exception e) {
            logger.warn("Error while reading message", e);
            IoUtils.safeClose(channel);
            polledBuffer.free();
        }
    }

    private void dispatch(TcpConnection tcpConnection, Pooled<ByteBuffer> pooledBuffer) {
        try {
            tcpConnection.incrementMessageReceived();
            tcpConnection.worker().execute(() -> handleEvent(tcpConnection, pooledBuffer));
        } catch (Exception e) {
            logger.error("Event handler threw an exception", e);
        }
    }

    private void handleEvent(TcpConnection tcpConnection, Pooled<ByteBuffer> pooledBuffer) {
        handler.onEvent(tcpConnection, pooledBuffer.getResource());
    }
}
