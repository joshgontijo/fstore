package io.joshworks.lsm.server.messages;

import io.joshworks.fstore.log.Direction;

public class Iterate {

    public String namespace;
    public Direction direction;
    public String startKey;
    public String endKey;

}
