package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.cluster.messages.ClusterMessage;
import io.joshworks.eventry.server.cluster.messages.MessageError;
import org.jgroups.Address;

public class NodeMessage {

    public final Address address;
    public final ClusterMessage message;

    public NodeMessage(Address address, ClusterMessage message) {
        this.address = address;
        this.message = message;
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
