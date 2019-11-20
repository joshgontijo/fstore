package io.joshworks.fstore.server.cluster.messages;

public class StreamCreated {
    public final String stream;

    public StreamCreated(String stream) {
        this.stream = stream;
    }
}
