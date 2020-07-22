package io.joshworks.es.async;

public class WriteEvent {

    public String stream;
    public String type;
    public int version; //set internally
    public int expectedVersion;
    public long timestamp;
    public short attributes;
    public byte[] data;
    public byte[] metadata;

}
