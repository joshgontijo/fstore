package io.joshworks.es2.writer;

import java.nio.ByteBuffer;

public interface Writer {

    long write(ByteBuffer src);

}
