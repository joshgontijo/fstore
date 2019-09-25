package io.joshworks.fstore.es.shared.messages;

public class ReadStream {

    public String stream;
    public int startVersion;
    public int limit;

    public ReadStream() {
    }

    public ReadStream(String stream, int startVersion, int limit) {
        this.stream = stream;
        this.startVersion = startVersion;
        this.limit = limit;
    }
}
