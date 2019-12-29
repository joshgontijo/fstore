package io.joshworks;

import java.util.List;

public interface ReplicationRpc {

    void createIterator(long position, String nodeId);

    List<Record> fetch(String nodeId);

    long position(String nodeId);

}
