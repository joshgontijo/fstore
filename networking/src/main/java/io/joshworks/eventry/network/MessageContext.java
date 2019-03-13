package io.joshworks.eventry.network;

import org.jgroups.Address;

public class MessageContext {

    public final ClusterMessage message;
    private final Address src;

    public MessageContext(ClusterMessage message, Address src) {
        this.message = message;
        this.src = src;
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

    public Address src() {
        return src;
    }
}
