package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

public interface Mapper {

    int apply(ByteBuffer b);

}
