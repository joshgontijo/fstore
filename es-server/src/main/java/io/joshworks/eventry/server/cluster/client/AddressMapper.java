package io.joshworks.eventry.server.cluster.client;

import org.jgroups.Address;

import java.util.function.Function;

public class AddressMapper {

    private final Function<String, Address> toAddress;
    private final Function<Address, String> toUuid;

    public AddressMapper(Function<String, Address> toAddress, Function<Address, String> toUuid) {
        this.toAddress = toAddress;
        this.toUuid = toUuid;
    }

    public String toUuid(Address address) {
        String uuid = toUuid.apply(address);
        if(uuid == null) {
            throw new IllegalStateException("No uuid found for address: " + address);
        }
        return uuid;
    }

    public Address toAddress(String uuid) {
        Address address = toAddress.apply(uuid);
        if(address == null) {
            throw new IllegalStateException("No uuid found for address: " + uuid);
        }
        return address;
    }
}
