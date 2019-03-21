package io.joshworks.eventry.network;

import org.jgroups.Address;

public class MulticastResponse {

    private final Address address; //FIXME REPLACE WITH NODE_ID
    private final ClusterMessage message;

    public MulticastResponse(Address address, ClusterMessage message) {
        this.address = address;
        this.message = message;
    }

    public Address address() {
        throw new UnsupportedOperationException("ADDRESS IS NOT SUPPORTED !!!!");
    }

    public <T extends ClusterMessage> T message() {
        if(message instanceof NullMessage) {
            return null;
        }
        return (T) message;
    }
}
