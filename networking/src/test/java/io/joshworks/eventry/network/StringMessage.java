package io.joshworks.eventry.network;

public class StringMessage implements ClusterMessage {

    public final String message;

    private StringMessage(String message) {
        this.message = message;
    }

    public static StringMessage of(String message) {
        return new StringMessage(message);
    }

}
