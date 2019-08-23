package io.joshworks.eventry.network.tcp;

import io.joshworks.eventry.network.tcp.client.InternalClientEventHandler;
import io.joshworks.eventry.network.tcp.client.KeepAliveConduit;
import io.joshworks.eventry.network.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.eventry.network.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.eventry.network.tcp.conduits.ConduitPipeline;
import io.joshworks.eventry.network.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.eventry.network.tcp.internal.KeepAlive;
import io.joshworks.eventry.network.tcp.internal.Response;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class TcpMessageClient {

    private final InetSocketAddress bindAddress;
    private final long keepAliveInterval;
    private final IoFuture<StreamConnection> connFuture;
    private final Consumer<TcpConnection> onClose;
    private final EventHandler eventHandler;
    private final XnioWorker worker;

    private final BufferPool writePool;
    private final SimpleBufferPool readPool;
    private transient TcpClientConnection tcpConnection;
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    private final Map<Long, Response> responseTable = new ConcurrentHashMap<>();


    public TcpMessageClient(OptionMap options, InetSocketAddress bindAddress, Set<Class> registeredTypes, int bufferSize, long keepAliveInterval, Consumer<TcpConnection> onClose, EventHandler handler) {
        this.bindAddress = bindAddress;
        this.keepAliveInterval = keepAliveInterval;
        this.onClose = onClose;
        this.eventHandler = handler;
        this.writePool = new ThreadLocalBufferPool(bufferSize, true);
        this.readPool =  new SimpleBufferPool(bufferSize, true);

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

    public TcpClientConnection connect(long timeout, TimeUnit unit) {
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
            tcpConnection = new TcpClientConnection(channel, writePool, responseTable);

            channel.setCloseListener(conn -> onClose.accept(tcpConnection));

            if (keepAliveInterval > 0) {
                new KeepAliveConduit(channel, keepAliveInterval); //adds to source and sink
            }

            SimpleBufferPool.BufferRef polled = readPool.allocateRef();

            ConduitPipeline pipeline = new ConduitPipeline(channel);
            pipeline.addMessageSource(conduit -> new FramingMessageSourceConduit(conduit, polled));
            pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));

            pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesSent));

            InternalClientEventHandler clientEventHandler = new InternalClientEventHandler(eventHandler, responseTable);
            ReadHandler readHandler = new ReadHandler(tcpConnection, clientEventHandler);
            pipeline.readListener(readHandler);

            channel.setCloseListener(conn -> {
                responseTable.clear();
                polled.free();
            });

            channel.getSourceChannel().resumeReads();
            channel.getSinkChannel().resumeWrites();
            connectLatch.countDown();
        }
    }

}
