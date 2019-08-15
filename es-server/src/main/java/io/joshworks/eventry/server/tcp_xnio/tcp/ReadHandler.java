package io.joshworks.eventry.server.tcp_xnio.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import org.xnio.Buffers;
import org.xnio.ChannelListener;
import org.xnio.StreamConnection;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.IOUtils.closeQuietly;
import static org.xnio._private.Messages.msg;

public class ReadHandler implements ChannelListener<ConduitStreamSourceChannel> {

    private final StreamConnection connection;
    private final Config config;
    private final ThreadLocalBufferPool readPool;


    public ReadHandler(StreamConnection connection, Config config, ThreadLocalBufferPool readPool) {
        this.connection = connection;
        this.config = config;
        this.readPool = readPool;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        try (readPool) {
            ByteBuffer receiveBuffer = readPool.allocate();

            int res;
            do {
                res = channel.read(receiveBuffer);
            } while (res > 0);
            if (receiveBuffer.position() < 4) {
                if (res == -1) {
                    receiveBuffer.clear();
                }
                return;
            }
            receiveBuffer.flip();
            try {
                final int length = receiveBuffer.getInt();
                if (length < 0 || length > receiveBuffer.capacity() - 4) {
                    Buffers.unget(receiveBuffer, 4);
                    throw msg.recvInvalidMsgLength(length);
                }
                if (receiveBuffer.remaining() < length) { //message lost
                    if (res == -1) {
                        receiveBuffer.clear();
                    } else {
                        Buffers.unget(receiveBuffer, 4);
                    }
                    // must be <= 0
                    return;
                }
                if (dst.hasRemaining()) {
                    return Buffers.copy(length, dst, receiveBuffer);
                } else {
                    Buffers.skip(receiveBuffer, length);
                    return 0;
                }
            } finally {
                if (res != -1) {
                    receiveBuffer.compact();
                    if (receiveBuffer.position() >= 4 && receiveBuffer.position() >= 4 + receiveBuffer.getInt(0)) {
                        // there's another packet ready to go
                        ready = true;
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            //TODO
        }


    }


    private void handle(ByteBuffer message) {
        try (readPool) {
            ByteBuffer buffer = readPool.allocate();
            int read = channel.read(buffer);
            if (read == 0) {
                return;
            }
            if (read == -1) {
                connection.close();
                return;
            }
            buffer.flip();
            dispatch(buffer);

        } catch (Exception e) {
            closeQuietly(connection);
            //TODO
            e.printStackTrace();
        }
    }

    private void dispatch(ByteBuffer buffer) {
        connection.getWorker().execute(() -> {
            try {

                ByteBuffer allocate1 = pool.allocate();
                allocate1.put(allocate);

                String message = new String(data.array(), data.position(), data.remaining());
                if ("aaaaaaaaaaaaaaaaaaa".equals(message)) {
                    System.out.println("Keep alive packet, dropping");
                    return;
                }

                process(connection.getSinkChannel(), message);
            } catch (Exception e) {
                closeQuietly(connection);
            }
        });
    }
}
