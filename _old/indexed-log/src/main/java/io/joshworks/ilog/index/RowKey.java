package io.joshworks.ilog.index;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface RowKey extends Comparator<ByteBuffer> {

    RowKey LONG = new LongRowKey();

    RowKey INT = new IntRowKey();

    int compare(ByteBuffer k1, int k1Idx, ByteBuffer k2, int k2Idx);

    default int compare(ByteBuffer o1, ByteBuffer o2) {
        return compare(o1, o1.position(), o2, o2.position());
    }

    int keySize();

}
