package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.es.shared.EventRecord;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class EventData  {

    public final EventRecord record;

    public EventData(EventRecord record) {
        this.record = record;
    }
}
