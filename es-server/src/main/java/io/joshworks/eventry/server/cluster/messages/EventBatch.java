package io.joshworks.eventry.server.cluster.messages;

public class EventBatch implements ClusterMessage {


    //implement array of EventRecord serializer

    @Override
    public byte[] toBytes() {
        throw new UnsupportedOperationException("NOT YET SUPPORTED");
    }

    @Override
    public int code() {
        return -99999;
    }
}
