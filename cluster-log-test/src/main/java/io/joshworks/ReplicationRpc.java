package io.joshworks;

import java.util.List;

public interface ReplicationRpc {

    void createIterator(String nodeId, Long lastSequence);

    List<Record> fetch(String nodeId);
}
