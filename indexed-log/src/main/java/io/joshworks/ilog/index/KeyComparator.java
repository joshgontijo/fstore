package io.joshworks.ilog.index;

import java.nio.ByteBuffer;

public interface KeyComparator {

    KeyComparator LONG = new LongKeyComparator();

    KeyComparator INT = new IntKeyComparator();

    int compare(ByteBuffer k1, ByteBuffer k2);

    int keySize();



}
