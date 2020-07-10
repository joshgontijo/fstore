package io.joshworks.ilog.index;

import io.joshworks.ilog.record.Record2;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface RowKey extends Comparator<ByteBuffer> {

    RowKey LONG = new LongRowKey();

    RowKey INT = new IntRowKey();

    int compare(ByteBuffer k1, int k1Idx, ByteBuffer k2, int k2Idx);

    int compare(Record2 rec1, Record2 rec2);

    int keySize();

}
