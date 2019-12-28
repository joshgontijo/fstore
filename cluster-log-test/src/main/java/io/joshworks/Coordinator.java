package io.joshworks;

import org.jgroups.Address;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Coordinator {

    private Address leader;
    //address - commitIndex
    private final Map<Address, AtomicLong> followers = new ConcurrentHashMap<>();

    private void onNodeConnected() {

    }


}
