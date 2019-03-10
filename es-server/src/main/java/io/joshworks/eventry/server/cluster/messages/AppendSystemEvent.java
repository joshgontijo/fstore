package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.log.EventRecord;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class AppendSystemEvent implements ClusterMessage {

    public final int expectedVersion;
    public final EventRecord record;

    public AppendSystemEvent(EventRecord record, int expectedVersion) {
        this.record = record;
        this.expectedVersion = expectedVersion;
    }
}
