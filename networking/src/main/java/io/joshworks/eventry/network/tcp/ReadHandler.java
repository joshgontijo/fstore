package io.joshworks.eventry.network.tcp;

import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.conduits.ConduitStreamSourceChannel;

public class ReadHandler implements ChannelListener<ConduitStreamSourceChannel> {

    private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);

    private final TcpConnection tcpConnection;
    private final SimpleBufferPool appBuffer = new SimpleBufferPool(4096, true);
    private final EventHandler handler;

    ReadHandler(TcpConnection tcpConnection, EventHandler handler) {
        this.tcpConnection = tcpConnection;
        this.handler = handler;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        try {
            if (!channel.isOpen()) {
                tcpConnection.close();
                return;
            }
            SimpleBufferPool.BufferRef ref = appBuffer.allocateRef();
            int read = channel.read(ref.buffer);
            if (read == 0) {
                return;
            }
            if (read == -1) {
                tcpConnection.close();
                return;
            }
            ref.buffer.flip();
            handle(tcpConnection, ref);
            channel.resumeReads();
        } catch (Exception e) {
            logger.warn("Error while reading message", e);
            IoUtils.safeClose(channel);
        }
    }

    private void handle(TcpConnection tcpConnection, SimpleBufferPool.BufferRef bufferRef) {
        //parsing happens in the io thread
        tcpConnection.incrementMessageReceived();
        tcpConnection.worker().execute(() -> {
            try {
                handler.onEvent(tcpConnection, bufferRef.buffer);

            } catch (Exception e) {
                logger.error("Event handler threw an exception", e);
            } finally {
                bufferRef.free();
            }
        });
    }
}
