package io.joshworks.lsm.server;

import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Replicate;
import io.joshworks.lsm.server.messages.Result;

public interface StoreNode {

    void put(Put msg);

    void replicate(Replicate msg);

    Result get(Get msg);

    void delete(Delete msg);

    void close();

    String id();
}
