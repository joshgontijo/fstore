package io.joshworks.fstore.server.cluster.messages;

public class IteratorCreated  {

    public final String iteratorId;

    public IteratorCreated(String iteratorId) {
        this.iteratorId = iteratorId;
    }

}
