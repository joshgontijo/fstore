package io.joshworks.eventry.server.tcp_xnio;

import io.joshworks.fstore.core.io.IOUtils;
import io.undertow.conduits.IdleTimeoutConduit;
import org.xnio.ByteBufferSlicePool;
import org.xnio.ChannelListener;
import org.xnio.ChannelPipe;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Pooled;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.Channels;
import org.xnio.channels.StreamChannel;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.conduits.AbstractStreamSinkConduit;
import org.xnio.conduits.FramingMessageSourceConduit;
import org.xnio.conduits.MessageStreamSourceConduit;
import org.xnio.conduits.StreamSinkConduit;
import org.xnio.conduits.StreamSourceConduit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class SimpleEchoServer {

    public static void main(String[] args) throws Exception {

        OptionMap options = OptionMap.builder()
                .set(Options.WORKER_IO_THREADS, 5)
                .set(Options.WORKER_TASK_CORE_THREADS, 3)
                .set(Options.WORKER_TASK_MAX_THREADS, 3)
                .set(Options.WORKER_NAME, "josh-worker")
                .set(Options.KEEP_ALIVE, true)
                .getMap();

        final XnioWorker worker = Xnio.getInstance().createWorker(options);

        ChannelPipe<StreamChannel, StreamChannel> fullDuplexPipe = worker.createFullDuplexPipe();

        // Create the server.
        AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(12346), new Acceptor(), OptionMap.EMPTY);

        // lets start accepting connections
        server.resumeAccepts();

        System.out.println("Listening on " + server.getLocalAddress());

    }

    private static class Acceptor implements ChannelListener<AcceptingChannel<StreamConnection>> {

        @Override
        public void handleEvent(AcceptingChannel<StreamConnection> channel) {
            try {
                System.out.println("Accepted: " + Thread.currentThread().getName());
                StreamConnection conn;
                // channel is ready to accept zero or more connections
                while ((conn = channel.accept()) != null) {
                    conn.setCloseListener(channel1 -> System.out.println("Conn Closed: " + channel1.getPeerAddress()));
                    StreamConnection connection = conn;
                    IdleTimeoutConduit conduit = new IdleTimeoutConduit(conn);
                    conduit.setIdleTimeout(5000);

//                    conn.getSourceChannel().setConduit(conduit);
//                    conn.getSinkChannel().setConduit(conduit);

                    Pooled<ByteBuffer> bufferPool = new ByteBufferSlicePool(4096, 4096 * 3).allocate();

                    StreamSourceConduit original = conduit;
//                    StreamSourceConduit original = conn.getSourceChannel().getConduit();
                    var frameConduit = new FramingMessageSourceConduit(original, bufferPool);

                    var wrapper = new MessageStreamSourceConduit(frameConduit);
                    conn.getSourceChannel().setConduit(wrapper);
                    conn.getSinkChannel().setConduit(new LoggingConduit(conn.getSinkChannel().getConduit()));

                    System.out.println("conn " + conn.getPeerAddress());
                    // stream channel has been conn at this stage.
                    conn.getSourceChannel().setReadListener(source -> {

                    });
                    conn.getSourceChannel().resumeReads();

                }
            } catch (IOException ignored) {
            }
        }
    }

    private static void process(StreamSinkChannel sink, String message) {
        try {
            System.out.println("Read: " + message + " -> THREAD_NAME: " + Thread.currentThread().getName());
//            Thread.sleep(3000);
            if(!sink.isOpen()) {
                sink.close();
                return;
            }
            Channels.writeBlocking(sink, ByteBuffer.wrap(("ECHO: " + message).getBytes(StandardCharsets.UTF_8)));
            Channels.flushBlocking(sink);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class LoggingConduit extends AbstractStreamSinkConduit<StreamSinkConduit> {

        protected LoggingConduit(StreamSinkConduit next) {
            super(next);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            System.out.println("Writing: " + Thread.currentThread().getName());
            return super.write(src);
        }
    }

}