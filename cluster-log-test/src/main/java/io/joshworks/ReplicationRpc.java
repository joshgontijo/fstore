package io.joshworks;

import java.util.List;

public interface ReplicationRpc {

    void createIterator(String nodeId, long lastSequence);

    List<Record> fetch(String nodeId, long lastSequence);

    long position(String nodeId);

}
