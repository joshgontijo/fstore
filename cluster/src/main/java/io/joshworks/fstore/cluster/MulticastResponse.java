package io.joshworks.fstore.cluster;

import org.jgroups.Address;

public class MulticastResponse {

    private final Address address; //FIXME REPLACE WITH NODE_ID
    private final Object message;

    public MulticastResponse(Address address, Object message) {
        this.address = address;
        this.message = message;
    }

    public Address address() {
        throw new UnsupportedOperationException("ADDRESS IS NOT SUPPORTED !!!!");
    }

    public <T> T message() {
        if (message instanceof NullMessage) {
            return null;
        }
        return (T) message;
    }
}
