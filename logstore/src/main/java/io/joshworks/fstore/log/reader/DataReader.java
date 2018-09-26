package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.Storage;

public interface DataReader {

    ByteBufferReference read(Storage storage, long position);

}
