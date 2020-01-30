package io.joshworks.ilog.index;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface KeyComparator extends Comparator<ByteBuffer> {

    KeyComparator LONG = new LongKeyComparator();

    KeyComparator INT = new IntKeyComparator();

    int compare(ByteBuffer k1, int k1Idx, ByteBuffer k2, int k2Idx);

    int keySize();

}
