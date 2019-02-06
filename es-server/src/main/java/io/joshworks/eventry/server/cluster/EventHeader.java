package io.joshworks.eventry.server.cluster;

import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.Supplier;

public class EventHeader extends Header {

    static final short HEADER_ID = 3501;

    private String eventType;

    public EventHeader() {
        //de-serialization
    }

    public EventHeader(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public short getMagicId() {
        return HEADER_ID;
    }

    @Override
    public Supplier<? extends Header> create() {
        return EventHeader::new;
    }

    @Override
    public int serializedSize() {
        return Util.size(eventType);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeObject(eventType, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        eventType = (String) Util.readObject(in);
    }

    public String eventType() {
        return eventType;
    }

}
