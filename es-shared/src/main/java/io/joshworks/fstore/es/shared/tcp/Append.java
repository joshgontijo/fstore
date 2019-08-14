package io.joshworks.fstore.es.shared.tcp;

import io.joshworks.fstore.es.shared.EventRecord;

public class Append extends Message {

    public final int expectedVersion;
    public final EventRecord record;

    public Append(int expectedVersion, EventRecord record) {
        this.expectedVersion = expectedVersion;
        this.record = record;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Append{");
        sb.append("expectedVersion=").append(expectedVersion);
        sb.append(", record=").append(record);
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}
