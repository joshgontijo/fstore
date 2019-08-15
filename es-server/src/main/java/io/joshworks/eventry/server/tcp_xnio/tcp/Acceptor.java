package io.joshworks.eventry.server.tcp_xnio.tcp;

import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.undertow.conduits.IdleTimeoutConduit;
import org.xnio.ByteBufferSlicePool;
import org.xnio.ChannelListener;
import org.xnio.Pooled;
import org.xnio.StreamConnection;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.FramingMessageSourceConduit;
import org.xnio.conduits.MessageStreamSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Acceptor implements ChannelListener<AcceptingChannel<StreamConnection>> {

    private final Config config;
    private final ThreadLocalBufferPool bufferPool;

    Acceptor(Config config, ThreadLocalBufferPool bufferPool) {
        this.config = config;
        this.bufferPool = bufferPool;
    }

    @Override
    public void handleEvent(AcceptingChannel<StreamConnection> channel) {
        try {
            StreamConnection conn;
            while ((conn = channel.accept()) != null) {
                conn.setCloseListener(ch -> config.onClose.accept(ch));

                IdleTimeoutConduit conduit = new IdleTimeoutConduit(conn);
                conduit.setIdleTimeout(config.timeout);

                conn.getSinkChannel().setConduit(conduit);

                Pooled<ByteBuffer> bufferPool = new ByteBufferSlicePool(4096, 4096 * 3).allocate();
                var frameConduit = new FramingMessageSourceConduit(conduit, bufferPool);
                var wrapper = new MessageStreamSourceConduit(frameConduit);

                conn.getSourceChannel().setConduit(wrapper);

                conn.getSourceChannel().setReadListener(new ReadHandler(conn, config, bufferPool));
                conn.getSourceChannel().resumeReads();

            }
        } catch (IOException ignored) {

        }
    }
}


//ByteBuffer data = ByteBuffer.allocate(512);
//                    try {
//                        int read = source.read(data);
//                        if(read == 0) {
//                            return;
//                        }
//                        if(read == - 1) {
//                            connection.close();
//                            return;
//                        }
//                        data.flip();
//                        String message = new String(data.array(), data.position(), data.remaining());
//                        if("aaaaaaaaaaaaaaaaaaa".equals(message)) {
//                            System.out.println("Keep alive packet, dropping");
//                            return;
//                        }
//                        source.getWorker().execute(() -> {
//                            try {
//                                process(connection.getSinkChannel(), message);
//
//                            } catch (Exception e) {
//                                IOUtils.closeQuietly(connection);
//                            }
//                        });