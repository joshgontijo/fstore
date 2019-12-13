package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.joshworks.fstore.serializer.kryo.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.nio.ByteBuffer;

public class ReadHandler implements ChannelListener<ConduitStreamSourceChannel> {

    private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);

    private final TcpConnection tcpConnection;
    private final BufferPool appBuffer = new ThreadLocalBufferPool("tcp-appBuffer-pool", 4096 * 2, true);
    private final EventHandler handler;

    public ReadHandler(TcpConnection tcpConnection, EventHandler handler) {
        this.tcpConnection = tcpConnection;
        this.handler = handler;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        try (appBuffer) {
            ByteBuffer buffer = appBuffer.allocate();
            int read;
            while ((read = channel.read(buffer)) > 0) {
                buffer.flip();
                handle(tcpConnection, buffer);
                buffer.clear();
            }
            if (read == -1) {
                IoUtils.safeClose(channel);
            }

        } catch (Exception e) {
            logger.warn("Error while reading message", e);
            IoUtils.safeClose(channel);
        }
    }

    private void handle(TcpConnection tcpConnection, ByteBuffer buffer) {
        Object object = parse(buffer);
        try {
            tcpConnection.incrementMessageReceived();
            handler.onEvent(tcpConnection, object);
        } catch (Exception e) {
            logger.error("Event handler threw an exception", e);
        }
    }

    private Object parse(ByteBuffer buffer) {
        try {
            return KryoSerializer.deserialize(buffer);
        } catch (Exception e) {
            throw new RuntimeException("Error while parsing data", e);
        }
    }
}
