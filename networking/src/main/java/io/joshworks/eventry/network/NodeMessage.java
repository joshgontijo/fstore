package io.joshworks.eventry.network;

import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.jgroups.Address;
import org.jgroups.blocks.Response;

public class NodeMessage {

    public final ClusterMessage message;
    boolean sent;
    private final Address dst;
    private final Response response;
    private final KryoStoreSerializer serializer;

    public NodeMessage(ClusterMessage message, Address dst, Response response, KryoStoreSerializer serializer) {
        this.message = message;
        this.dst = dst;
        this.response = response;
        this.serializer = serializer;
    }

    public <T> T get() {
        if (isError()) {
            throw new RuntimeException("Received error response from node");
        }
        return (T) message;
    }

    public boolean isEmpty() {
        return message == null;
    }

    public boolean isError() {
        return !isEmpty() && message instanceof MessageError;
    }
}
