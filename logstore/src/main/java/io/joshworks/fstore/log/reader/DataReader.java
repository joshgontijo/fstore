package io.joshworks.fstore.log.reader;

public interface DataReader {

    ByteBufferReference read(long position);

}
