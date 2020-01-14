package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.StupidPool;
import io.joshworks.fstore.tcp.client.ClientResponseHandler;
import io.joshworks.fstore.tcp.client.KeepAliveConduit;
import io.joshworks.fstore.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.fstore.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.fstore.tcp.conduits.ConduitPipeline;
import io.joshworks.fstore.tcp.conduits.FramingMessageSinkConduit;
import io.joshworks.fstore.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class TcpMessageClient {

    private final InetSocketAddress bindAddress;
    private final int maxMessageSize;
    private final long keepAliveInterval;
    private final IoFuture<StreamConnection> connFuture;
    private final Consumer<TcpConnection> onClose;
    private final EventHandler eventHandler;
    private final XnioWorker worker;

    private final StupidPool writePool;
    private final StupidPool readPool;
    private transient TcpClientConnection tcpConnection;
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    private final ResponseTable responseTable;
    private final boolean async;

    public TcpMessageClient(OptionMap options,
                            InetSocketAddress bindAddress,
                            int readPoolSize,
                            int writePoolSize,
                            int maxMessageSize,
                            long keepAliveInterval,
                            Consumer<TcpConnection> onClose,
                            EventHandler handler,
                            ResponseTable responseTable,
                            boolean async) {

        this.bindAddress = bindAddress;
        this.maxMessageSize = maxMessageSize;
        this.readPool = new StupidPool(readPoolSize, maxMessageSize);
        this.writePool = new StupidPool(writePoolSize, maxMessageSize);
        this.keepAliveInterval = keepAliveInterval;
        this.onClose = onClose;
        this.eventHandler = handler;
        this.responseTable = responseTable;
        this.async = async;


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

            ConduitPipeline pipeline = new ConduitPipeline(channel);
            pipeline.addMessageSource(conduit -> new FramingMessageSourceConduit(conduit, maxMessageSize, readPool));
            pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));

            pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesSent));
            pipeline.addStreamSink(FramingMessageSinkConduit::new);

            if (keepAliveInterval > 0) {
                new KeepAliveConduit(channel, keepAliveInterval); //adds to source and sink
            }

            ClientResponseHandler clientEventHandler = new ClientResponseHandler(eventHandler, responseTable);
            ReadHandler readHandler = new ReadHandler(tcpConnection, clientEventHandler, async);
            pipeline.readListener(readHandler);

            channel.setCloseListener(conn -> responseTable.clear());

            channel.getSourceChannel().resumeReads();
            channel.getSinkChannel().resumeWrites();
            connectLatch.countDown();
        }
    }

}
