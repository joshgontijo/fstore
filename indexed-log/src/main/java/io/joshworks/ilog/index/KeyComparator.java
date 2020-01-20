package io.joshworks.ilog.index;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface KeyComparator extends Comparator<ByteBuffer> {

    KeyComparator LONG = new LongKeyComparator();

    KeyComparator INT = new IntKeyComparator();

    int keySize();

}
