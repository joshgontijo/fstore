package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.network.ClusterMessage;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class EventData implements ClusterMessage {

    public final EventRecord record;

    public EventData(EventRecord record) {
        this.record = record;
    }
}
