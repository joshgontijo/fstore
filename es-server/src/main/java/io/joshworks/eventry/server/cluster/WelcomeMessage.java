package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.cluster.data.NetworkMessage;

public class WelcomeMessage extends NetworkMessage {

    public String message;

    public WelcomeMessage() {
    }

    public WelcomeMessage(String message) {
        this.message = message;
    }
}
