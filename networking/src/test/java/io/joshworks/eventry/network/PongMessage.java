package io.joshworks.eventry.network;

import java.util.Objects;

public class PongMessage implements ClusterMessage {

    private final String message = "PONG";

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PongMessage that = (PongMessage) o;
        return Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message);
    }
}
