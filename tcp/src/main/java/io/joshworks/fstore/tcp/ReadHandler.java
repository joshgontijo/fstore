package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.StupidPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.nio.ByteBuffer;

public class ReadHandler implements ChannelListener<ConduitStreamSourceChannel> {

    private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);

    private final TcpConnection tcpConnection;
    private final StupidPool pool = new StupidPool(100, 4096);
    private final EventHandler handler;
    private final boolean async;

    public ReadHandler(TcpConnection tcpConnection, EventHandler handler, boolean async) {
        this.tcpConnection = tcpConnection;
        this.handler = handler;
        this.async = async;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        ByteBuffer buffer = pool.allocate();
        try {
            int read;
            while ((read = channel.read(buffer)) > 0) {
                buffer.flip();
                dispatch(tcpConnection, buffer);
            }
            if (read == -1) {
                IoUtils.safeClose(channel);
                pool.free(buffer);
            }

        } catch (Exception e) {
            logger.warn("Error while reading message", e);
            IoUtils.safeClose(channel);
            pool.free(buffer);
        }
    }

    private void dispatch(TcpConnection tcpConnection, ByteBuffer buffer) {
        try {
            if (async) {
                tcpConnection.worker().execute(() -> handleEvent(tcpConnection, buffer));
            } else {
                handleEvent(tcpConnection, buffer);
            }
        } catch (Exception e) {
            logger.error("Event handler threw an exception", e);
        }
    }

    private void handleEvent(TcpConnection tcpConnection, ByteBuffer buffer) {
        try {
            tcpConnection.incrementMessageReceived();
            handler.onEvent(tcpConnection, buffer);
        } finally {
            pool.free(buffer);
        }
    }
}
