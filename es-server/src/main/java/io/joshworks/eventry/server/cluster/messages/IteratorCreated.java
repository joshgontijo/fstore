package io.joshworks.eventry.server.cluster.messages;

public class IteratorCreated  {

    public final String iteratorId;

    public IteratorCreated(String iteratorId) {
        this.iteratorId = iteratorId;
    }

}
