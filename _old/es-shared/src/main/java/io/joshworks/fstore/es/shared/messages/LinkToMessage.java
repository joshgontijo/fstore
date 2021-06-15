package io.joshworks.fstore.es.shared.messages;

public class LinkToMessage {
    public String stream;
    public int expectedVersion;

    public String originalStream;
    public String originalType;
    public int originalVersion;

    public LinkToMessage() {
    }

    public LinkToMessage(String stream, int expectedVersion, String originalStream, String originalType, int originalVersion) {
        this.stream = stream;
        this.expectedVersion = expectedVersion;
        this.originalStream = originalStream;
        this.originalType = originalType;
        this.originalVersion = originalVersion;
    }
}
