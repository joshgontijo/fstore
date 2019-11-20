package io.joshworks.fstore.cluster;

public class StringMessage  {

    public final String message;

    private StringMessage(String message) {
        this.message = message;
    }

    public static StringMessage of(String message) {
        return new StringMessage(message);
    }

}
