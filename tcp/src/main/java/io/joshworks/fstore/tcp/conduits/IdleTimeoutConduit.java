package io.joshworks.fstore.tcp.conduits;

import io.joshworks.fstore.tcp.internal.WorkerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Buffers;
import org.xnio.StreamConnection;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.channels.StreamSourceChannel;
import org.xnio.conduits.ReadReadyHandler;
import org.xnio.conduits.StreamSinkConduit;
import org.xnio.conduits.StreamSourceConduit;
import org.xnio.conduits.WriteReadyHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


public class IdleTimeoutConduit implements StreamSinkConduit, StreamSourceConduit {

    private static final Logger logger = LoggerFactory.getLogger(IdleTimeoutConduit.class);

    private static final int DELTA = 100;
    private final StreamSinkConduit sink;
    private final StreamSourceConduit source;
    private final StreamConnection connection;
    private final Consumer<StreamConnection> idleHandler;
    private volatile XnioExecutor.Key handle;
    private volatile long idleTimeout;
    private volatile long expireTime = -1;
    private volatile boolean timedOut = false;
    private volatile WriteReadyHandler writeReadyHandler;
    private volatile ReadReadyHandler readReadyHandler;

    public IdleTimeoutConduit(StreamConnection connection, Consumer<StreamConnection> idleHandler) {
        this.connection = connection;
        this.sink = connection.getSinkChannel().getConduit();
        this.source = connection.getSourceChannel().getConduit();
        setWriteReadyHandler(new WriteReadyHandler.ChannelListenerHandler<>(connection.getSinkChannel()));
        setReadReadyHandler(new ReadReadyHandler.ChannelListenerHandler<>(connection.getSourceChannel()));
        this.idleHandler = idleHandler;
    }    private final Runnable timeoutCommand = new Runnable() {
        @Override
        public void run() {
            handle = null;
            if (expireTime == -1) {
                return;
            }
            long current = System.currentTimeMillis();
            if (current < expireTime) {
                //timeout has been bumped, re-schedule
                handle = WorkerUtils.executeAfter(getWriteThread(), timeoutCommand, (expireTime - current) + DELTA, TimeUnit.MINUTES);
                return;
            }

            logger.debug("Timing out channel due to inactivity");
            timedOut = true;
            doClose();
            if (sink.isWriteResumed()) {
                if (writeReadyHandler != null) {
                    writeReadyHandler.writeReady();
                }
            }
            if (source.isReadResumed()) {
                if (readReadyHandler != null) {
                    readReadyHandler.readReady();
                }
            }
        }
    };

    public IdleTimeoutConduit(StreamConnection connection) {
        this(connection, conn -> {
        });
    }

    private static void safeClose(final StreamSourceConduit sink) {
        try {
            sink.terminateReads();
        } catch (IOException e) {
        }
    }

    private static void safeClose(final StreamSinkConduit sink) {
        try {
            sink.truncateWrites();
        } catch (IOException e) {
        }
    }

    protected void doClose() {
        idleHandler.accept(connection);
        safeClose(sink);
        safeClose(source);
    }

    private void handleIdleTimeout() throws ClosedChannelException {
        if (timedOut) {
            return;
        }
        long idleTimeout = this.idleTimeout;
        if (idleTimeout <= 0) {
            return;
        }
        long currentTime = System.currentTimeMillis();
        long expireTimeVar = expireTime;
        if (expireTimeVar != -1 && currentTime > expireTimeVar) {
            timedOut = true;
            doClose();
            throw new ClosedChannelException();
        }
        expireTime = currentTime + idleTimeout;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        handleIdleTimeout();
        int w = sink.write(src);
        return w;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        handleIdleTimeout();
        long w = sink.write(srcs, offset, length);
        return w;
    }

    @Override
    public int writeFinal(ByteBuffer src) throws IOException {
        handleIdleTimeout();
        int w = sink.writeFinal(src);
        if (source.isReadShutdown() && !src.hasRemaining()) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
        return w;
    }

    @Override
    public long writeFinal(ByteBuffer[] srcs, int offset, int length) throws IOException {
        handleIdleTimeout();
        long w = sink.writeFinal(srcs, offset, length);
        if (source.isReadShutdown() && !Buffers.hasRemaining(srcs, offset, length)) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
        return w;
    }

    @Override
    public long transferTo(long position, long count, FileChannel target) throws IOException {
        handleIdleTimeout();
        long w = source.transferTo(position, count, target);
        if (sink.isWriteShutdown() && w == -1) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
        return w;
    }

    @Override
    public long transferTo(long count, ByteBuffer throughBuffer, StreamSinkChannel target) throws IOException {
        handleIdleTimeout();
        long w = source.transferTo(count, throughBuffer, target);
        if (sink.isWriteShutdown() && w == -1) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
        return w;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        handleIdleTimeout();
        long r = source.read(dsts, offset, length);
        if (sink.isWriteShutdown() && r == -1) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
        return r;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        handleIdleTimeout();
        int r = source.read(dst);
        if (sink.isWriteShutdown() && r == -1) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
        return r;
    }

    @Override
    public long transferFrom(FileChannel src, long position, long count) throws IOException {
        handleIdleTimeout();
        return sink.transferFrom(src, position, count);
    }

    @Override
    public long transferFrom(StreamSourceChannel source, long count, ByteBuffer throughBuffer) throws IOException {
        handleIdleTimeout();
        return sink.transferFrom(source, count, throughBuffer);
    }

    @Override
    public void suspendReads() {
        source.suspendReads();
        XnioExecutor.Key handle = this.handle;
        if (handle != null && !isWriteResumed()) {
            handle.remove();
            this.handle = null;
        }
    }

    @Override
    public void terminateReads() throws IOException {
        source.terminateReads();
        if (sink.isWriteShutdown()) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
    }

    @Override
    public boolean isReadShutdown() {
        return source.isReadShutdown();
    }

    @Override
    public void resumeReads() {
        source.resumeReads();
        handleResumeTimeout();
    }

    @Override
    public boolean isReadResumed() {
        return source.isReadResumed();
    }

    @Override
    public void wakeupReads() {
        source.wakeupReads();
        handleResumeTimeout();
    }

    @Override
    public void awaitReadable() throws IOException {
        source.awaitReadable();
    }

    @Override
    public void awaitReadable(long time, TimeUnit timeUnit) throws IOException {
        source.awaitReadable(time, timeUnit);
    }

    @Override
    public XnioIoThread getReadThread() {
        return source.getReadThread();
    }

    @Override
    public void setReadReadyHandler(ReadReadyHandler handler) {
        this.readReadyHandler = handler;
        source.setReadReadyHandler(handler);
    }

    @Override
    public void terminateWrites() throws IOException {
        sink.terminateWrites();
        if (source.isReadShutdown()) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
    }

    @Override
    public boolean isWriteShutdown() {
        return sink.isWriteShutdown();
    }

    @Override
    public void resumeWrites() {
        sink.resumeWrites();
        handleResumeTimeout();
    }

    @Override
    public void suspendWrites() {
        sink.suspendWrites();
        XnioExecutor.Key handle = this.handle;
        if (handle != null && !isReadResumed()) {
            handle.remove();
            this.handle = null;
        }

    }

    @Override
    public void wakeupWrites() {
        sink.wakeupWrites();
        handleResumeTimeout();
    }

    private void handleResumeTimeout() {
        long timeout = getIdleTimeout();
        if (timeout <= 0) {
            return;
        }
        long currentTime = System.currentTimeMillis();
        long newExpireTime = currentTime + timeout;
        boolean shorter = newExpireTime < expireTime;
        if (shorter && handle != null) {
            handle.remove();
            handle = null;
        }
        expireTime = newExpireTime;
        XnioExecutor.Key key = handle;
        if (key == null) {
            handle = WorkerUtils.executeAfter(getWriteThread(), timeoutCommand, timeout, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean isWriteResumed() {
        return sink.isWriteResumed();
    }

    @Override
    public void awaitWritable() throws IOException {
        sink.awaitWritable();
    }

    @Override
    public void awaitWritable(long time, TimeUnit timeUnit) throws IOException {
        sink.awaitWritable();
    }

    @Override
    public XnioIoThread getWriteThread() {
        return sink.getWriteThread();
    }

    @Override
    public void setWriteReadyHandler(WriteReadyHandler handler) {
        this.writeReadyHandler = handler;
        sink.setWriteReadyHandler(handler);
    }

    @Override
    public void truncateWrites() throws IOException {
        sink.truncateWrites();
        if (source.isReadShutdown()) {
            if (handle != null) {
                handle.remove();
                handle = null;
            }
        }
    }

    @Override
    public boolean flush() throws IOException {
        return sink.flush();
    }

    @Override
    public XnioWorker getWorker() {
        return sink.getWorker();
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
        if (idleTimeout > 0) {
            expireTime = System.currentTimeMillis() + idleTimeout;
            if (isReadResumed() || isWriteResumed()) {
                handleResumeTimeout();
            }
        } else {
            expireTime = -1;
        }
    }


}
