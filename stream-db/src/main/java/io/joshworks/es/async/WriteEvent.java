package io.joshworks.es.async;

import java.nio.ByteBuffer;

public class WriteEvent {

    public String stream;
    public String type;
    public int version;
    public long timestamp;
    public short attributes;
    public byte[] data;
    public byte[] metadata;

    //helpers
    public int expectedVersion;
    public long streamHash;

    //log write
    public ByteBuffer serialized;

    //after log write
    public long logPos;

}
