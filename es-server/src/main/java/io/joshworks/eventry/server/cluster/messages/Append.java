package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.network.ClusterMessage;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class Append implements ClusterMessage {

    public final EventRecord event;
    public final int expectedVersion;

    public Append(EventRecord event, int expectedVersion) {

        this.event = event;
        this.expectedVersion = expectedVersion;
    }

    @Override
    public String toString() {
        return "Append{" + "event=" + event +
                ", expectedVersion=" + expectedVersion +
                '}';
    }
}
