package io.joshworks.eventry.network;

import org.jgroups.Address;

public class MulticastResponse {

    private final Address address;
    private final ClusterMessage message;

    public MulticastResponse(Address address, ClusterMessage message) {
        this.address = address;
        this.message = message;
    }
}
