package io.joshworks.fstore.cluster;

import org.jgroups.Address;

public class NodeMessage<T> {

    private final Address sender;
    private final Object data;


    public NodeMessage(Address sender, Object data) {
        this.sender = sender;
        this.data = data;
    }

    public Address sender() {
        return sender;
    }

    public T data() {
        return (T) data;
    }
}
