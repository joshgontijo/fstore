package io.joshworks.fstore.es.shared.messages;

import io.joshworks.fstore.es.shared.EventRecord;

public class Append {

    public int expectedVersion;
    public EventRecord record;

    public Append() {
    }

    public Append(int expectedVersion, EventRecord record) {
        this.expectedVersion = expectedVersion;
        this.record = record;
    }

}
