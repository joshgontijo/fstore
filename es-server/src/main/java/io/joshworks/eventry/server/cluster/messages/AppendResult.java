package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class AppendResult implements ClusterMessage {

    public final boolean success;
    public final long timestamp;
    public final int version;

    public AppendResult(boolean success, long timestamp, int version) {
        this.success = success;
        this.timestamp = timestamp;
        this.version = version;
    }

    @Override
    public String toString() {
        return "AppendResult{" + "success=" + success +'}';
    }
}