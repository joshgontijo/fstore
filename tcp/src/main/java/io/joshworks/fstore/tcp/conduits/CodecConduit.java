package io.joshworks.fstore.tcp.conduits;

import org.xnio.StreamConnection;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;
import org.xnio.conduits.MessageSinkConduit;
import org.xnio.conduits.MessageSourceConduit;
import org.xnio.conduits.ReadReadyHandler;
import org.xnio.conduits.WriteReadyHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * NOT THREAD SAFE !
 * When using this class all channel writes must be synhronized}
 */
public class CodecConduit implements MessageSinkConduit, MessageSourceConduit {

    private final ConduitStreamSourceChannel source;
    private final ConduitStreamSinkChannel sink;


    protected CodecConduit(StreamConnection connection) {
        this.source = connection.getSourceChannel();
        this.sink = connection.getSinkChannel();
    }

    @Override
    public boolean send(ByteBuffer src) throws IOException {
        return false;
    }

    @Override
    public boolean send(ByteBuffer[] srcs, int offs, int len) throws IOException {
        return false;
    }

    @Override
    public boolean sendFinal(ByteBuffer src) throws IOException {
        return false;
    }

    @Override
    public boolean sendFinal(ByteBuffer[] srcs, int offs, int len) throws IOException {
        return false;
    }

    @Override
    public int receive(ByteBuffer dst) throws IOException {
        return 0;
    }

    @Override
    public long receive(ByteBuffer[] dsts, int offs, int len) throws IOException {
        return 0;
    }

    @Override
    public void terminateWrites() throws IOException {

    }

    @Override
    public boolean isWriteShutdown() {
        return false;
    }

    @Override
    public void resumeWrites() {

    }

    @Override
    public void suspendWrites() {

    }

    @Override
    public void wakeupWrites() {

    }

    @Override
    public boolean isWriteResumed() {
        return false;
    }

    @Override
    public void awaitWritable() throws IOException {

    }

    @Override
    public void awaitWritable(long time, TimeUnit timeUnit) throws IOException {

    }

    @Override
    public XnioIoThread getWriteThread() {
        return null;
    }

    @Override
    public void setWriteReadyHandler(WriteReadyHandler handler) {

    }

    @Override
    public void truncateWrites() throws IOException {

    }

    @Override
    public boolean flush() throws IOException {
        return false;
    }

    @Override
    public void terminateReads() throws IOException {

    }

    @Override
    public boolean isReadShutdown() {
        return false;
    }

    @Override
    public void resumeReads() {

    }

    @Override
    public void suspendReads() {

    }

    @Override
    public void wakeupReads() {

    }

    @Override
    public boolean isReadResumed() {
        return false;
    }

    @Override
    public void awaitReadable() throws IOException {

    }

    @Override
    public void awaitReadable(long time, TimeUnit timeUnit) throws IOException {

    }

    @Override
    public XnioIoThread getReadThread() {
        return null;
    }

    @Override
    public void setReadReadyHandler(ReadReadyHandler handler) {

    }

    @Override
    public XnioWorker getWorker() {
        return null;
    }
}
