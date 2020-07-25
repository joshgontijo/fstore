package io.joshworks.es.events;

public class WriteEvent {

    public String stream;
    public String type;
    public int expectedVersion = -1;
    public byte attributes;
    public byte[] data;
    public byte[] metadata;

}
