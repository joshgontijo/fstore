package io.joshworks.fstore.cluster;

import java.util.Objects;

public class PingMessage {

    private final String message = "PING";

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PingMessage that = (PingMessage) o;
        return Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message);
    }
}
