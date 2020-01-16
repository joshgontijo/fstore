package io.joshworks.fstore.tcp.client;

import io.joshworks.fstore.tcp.internal.WorkerUtils;
import io.joshworks.fstore.tcp.internal.KeepAlive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Buffers;
import org.xnio.IoUtils;
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
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

public class KeepAliveConduit implements StreamSinkConduit, StreamSourceConduit {

    private static final Logger logger = LoggerFactory.getLogger(KeepAliveConduit.class);
    private volatile XnioExecutor.Key handle;
    private StreamConnection connection;
    private volatile long interval;
    private volatile long lastActivity = System.nanoTime();

    private final StreamSinkConduit sink;
    private final StreamSourceConduit source;

    private volatile WriteReadyHandler writeReadyHandler;
    private volatile ReadReadyHandler readReadyHandler;
    private final ByteBuffer keepAliveData = ByteBuffer.wrap(KeepAlive.DATA);

    private final Runnable timeoutCommand = new Runnable() {
        @Override
        public void run() {
            handle.remove();
            handle = null;
            if (!connection.isOpen()) {
                return;
            }
            doSend();
            handle = WorkerUtils.executeAfter(getWriteThread(), timeoutCommand, interval, TimeUnit.MILLISECONDS);
        }
    };

    protected void doSend() {
        try {
            if (System.nanoTime() - lastActivity < TimeUnit.MILLISECONDS.toNanos(interval)) {
                return;
            }
            logger.debug("Sending keep alive");
            sink.write(keepAliveData.slice());

        } catch (Exception e) {
            Throwable t = e.getCause() == null ? e : e.getCause();
            logger.error("Failed to send keep alive to " + connection.getPeerAddress() + ", disconnecting", t);
            IoUtils.safeClose(connection);
            connection.getWorker().shutdown();
        }
    }

    public KeepAliveConduit(StreamConnection connection, long interval) {
        this.sink = connection.getSinkChannel().getConduit();
        this.source = connection.getSourceChannel().getConduit();
        this.connection = connection;
        this.interval = interval;
        setWriteReadyHandler(new WriteReadyHandler.ChannelListenerHandler<>(connection.getSinkChannel()));
        setReadReadyHandler(new ReadReadyHandler.ChannelListenerHandler<>(connection.getSourceChannel()));
        this.handle = WorkerUtils.executeAfter(getWriteThread(), timeoutCommand, interval, TimeUnit.MILLISECONDS);
    }

    private void updateActivityTimestamp() {
        lastActivity = System.nanoTime();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        updateActivityTimestamp();
        return sink.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        updateActivityTimestamp();
        return sink.write(srcs, offset, length);
    }

    @Override
    public int writeFinal(ByteBuffer src) throws IOException {
        updateActivityTimestamp();
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
        updateActivityTimestamp();
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
        updateActivityTimestamp();
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
        updateActivityTimestamp();
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
        updateActivityTimestamp();
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
        updateActivityTimestamp();
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
        updateActivityTimestamp();
        return sink.transferFrom(src, position, count);
    }

    @Override
    public long transferFrom(StreamSourceChannel source, long count, ByteBuffer throughBuffer) throws IOException {
        updateActivityTimestamp();
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
        //TODO investigate
        lastActivity = System.nanoTime();
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

    public long getInterval() {
        return interval;
    }

}