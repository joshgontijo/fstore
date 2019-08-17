package io.joshworks.eventry.network.tcp_xnio.tcp;

import io.joshworks.eventry.server.tcp_xnio.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.eventry.server.tcp_xnio.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.eventry.server.tcp_xnio.tcp.conduits.ConduitPipeline;
import io.joshworks.eventry.server.tcp_xnio.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.eventry.server.tcp_xnio.tcp.internal.KeepAlive;
import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.xnio.ByteBufferSlicePool;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Pooled;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class XTcpClient {

    private final InetSocketAddress bindAddress;
    private final int bufferSize;
    private final long keepAliveInterval;
    private final IoFuture<StreamConnection> connFuture;
    private final Consumer<TcpConnection> onClose;
    private final EventHandler eventHandler;
    private final XnioWorker worker;

    private final SimpleBufferPool messagePool;
    private final ByteBufferSlicePool readPool;
    private transient TcpConnection tcpConnection;
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    public XTcpClient(OptionMap options, InetSocketAddress bindAddress, Set<Class> registeredTypes, int bufferSize, long keepAliveInterval, Consumer<TcpConnection> onClose, EventHandler handler) {
        this.bindAddress = bindAddress;
        this.bufferSize = bufferSize;
        this.keepAliveInterval = keepAliveInterval;
        this.onClose = onClose;
        this.eventHandler = handler;
        this.messagePool = new SimpleBufferPool(bufferSize, true);
        this.readPool = new ByteBufferSlicePool(bufferSize, bufferSize * 20);

        registeredTypes.add(KeepAlive.class);
        KryoStoreSerializer.register(registeredTypes.toArray(Class[]::new));

        try {
            final Xnio xnio = Xnio.getInstance();
            this.worker = xnio.createWorker(options);
            this.connFuture = worker.openStreamConnection(bindAddress, new ConnectionAccepted(), OptionMap.builder().set(Options.WORKER_IO_THREADS, 1).getMap());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TcpConnection connect(long timeout, TimeUnit unit) {
        try {
            IoFuture.Status status = connFuture.awaitInterruptibly(timeout, unit);
            if (IoFuture.Status.FAILED.equals(status)) {
                throw connFuture.getException();
            }
            if (!connectLatch.await(timeout, unit)) {
                throw new TimeoutException("Connection timeout: " + bindAddress.getAddress().getHostAddress());
            }
            return tcpConnection;

        } catch (Exception e) {
            worker.shutdown();
            throw new RuntimeException("Failed to connect", e);
        }

    }

    private class ConnectionAccepted implements ChannelListener<StreamConnection> {

        @Override
        public void handleEvent(StreamConnection channel) {
            tcpConnection = new TcpConnection(channel, messagePool);

            channel.setCloseListener(conn -> onClose.accept(tcpConnection));

            new KeepAliveConduit(channel, keepAliveInterval); //adds to source and sink

            Pooled<ByteBuffer> polled = readPool.allocate();

            ConduitPipeline pipeline = new ConduitPipeline(channel);
            pipeline.addMessageSource(conduit -> new FramingMessageSourceConduit(conduit, false, polled));
            pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));

            pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesReceived));

            pipeline.readListener(new ReadHandler(tcpConnection, new KryoEventHandler(eventHandler)));

            channel.getSourceChannel().resumeReads();
            connectLatch.countDown();
        }
    }

}
