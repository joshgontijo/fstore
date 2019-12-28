package io.joshworks.fstore.cluster;

import org.jgroups.Address;

import java.util.function.BiConsumer;

public class LoggingInterceptor implements BiConsumer<Address, Object> {

    @Override
    public void accept(Address addr, Object entity) {
        System.err.println(String.format("<<<<<< %s => %s", addr, entity));
    }
}
