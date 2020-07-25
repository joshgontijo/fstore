package io.joshworks.es.writer;

public class WriteEvent {

    public String stream;
    public String type;
    public int version; //set internally
    public int expectedVersion = -1;
    public long timestamp;
    public byte attributes;
    public byte[] data;
    public byte[] metadata;

}
