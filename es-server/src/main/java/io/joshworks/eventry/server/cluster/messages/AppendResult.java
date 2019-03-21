package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class AppendResult implements ClusterMessage {

    public final boolean success;

    public AppendResult(boolean success) {
        this.success = success;
    }

    @Override
    public String toString() {
        return "AppendResult{" + "success=" + success +'}';
    }
}
