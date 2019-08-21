package io.joshworks.eventry.network.tcp.conduits;

import org.xnio.ChannelListener;
import org.xnio.StreamConnection;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;
import org.xnio.conduits.MessageSinkConduit;
import org.xnio.conduits.MessageSourceConduit;
import org.xnio.conduits.MessageStreamSinkConduit;
import org.xnio.conduits.MessageStreamSourceConduit;
import org.xnio.conduits.StreamSinkConduit;
import org.xnio.conduits.StreamSourceConduit;

import java.util.function.Function;

public class ConduitPipeline {

    private final ConduitStreamSinkChannel sink;
    private final ConduitStreamSourceChannel source;
    private final StreamConnection conn;

    public ConduitPipeline(StreamConnection conn) {
        sink = conn.getSinkChannel();
        source = conn.getSourceChannel();
        this.conn = conn;
    }


    public <D extends StreamSourceConduit> ConduitPipeline addStreamSource(Function<StreamSourceConduit, D> nextConduit) {
        StreamSourceConduit conduit = source.getConduit();
        D next = nextConduit.apply(conduit);
        source.setConduit(next);
        return this;
    }

    public <D extends StreamSinkConduit> ConduitPipeline addStreamSink(Function<StreamSinkConduit, D> nextConduit) {
        StreamSinkConduit conduit = sink.getConduit();
        D next = nextConduit.apply(conduit);
        sink.setConduit(next);
        return this;
    }

    public <D extends MessageSourceConduit> ConduitPipeline addMessageSource(Function<StreamSourceConduit, D> nextConduit) {
        StreamSourceConduit conduit = source.getConduit();
        D next = nextConduit.apply(conduit);
        source.setConduit(new MessageStreamSourceConduit(next));
        return this;
    }

    public <D extends MessageSinkConduit> ConduitPipeline addMessageSink(Function<StreamSinkConduit, D> nextConduit) {
        StreamSinkConduit conduit = sink.getConduit();
        D next = nextConduit.apply(conduit);
        sink.setConduit(new MessageStreamSinkConduit(next));
        return this;
    }


    public ConduitPipeline readListener(final ChannelListener<? super ConduitStreamSourceChannel> listener) {
        source.setReadListener(listener);
        return this;
    }

    public ConduitPipeline writeListener(final ChannelListener<? super ConduitStreamSinkChannel> listener) {
        sink.setWriteListener(listener);
        return this;
    }

    public ConduitPipeline closeListener(final ChannelListener<? super StreamConnection> listener) {
        conn.setCloseListener(listener);
        return this;
    }


}
