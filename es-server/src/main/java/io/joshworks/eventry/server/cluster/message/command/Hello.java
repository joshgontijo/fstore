//package io.joshworks.eventry.server.cluster.message.command;
//
//import io.joshworks.eventry.log.EventRecord;
//import io.joshworks.eventry.server.cluster.message.NodeJoined;
//import org.jgroups.Address;
//
//public class Hello extends ClusterCommand {
//
//    private final String id;
//
//    public Hello(String id) {
//        this.id = id;
//    }
//
//    @Override
//    public EventRecord toEvent(Address address) {
//
//        return NodeJoined.create(address, id);
//    }
//}
