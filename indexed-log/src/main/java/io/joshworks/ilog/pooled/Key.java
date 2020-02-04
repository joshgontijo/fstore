package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.index.KeyComparator;

public class Key extends Pooled {

    Key(ObjectPool.Pool<? extends Pooled> pool, int size, boolean direct) {
        super(pool, size, direct);
    }

    public int compareTo(Key key, KeyComparator comparator) {
        return comparator.compare(this, key);
    }

    public void copyFrom(Pooled pooled, int offset, int count) {
        Buffers.copy(pooled.data, offset, count, data);
    }

    public long getLong(int idx) {
        return data.getLong(idx);
    }

    public int getInt(int idx) {
        return data.getInt(idx);
    }

    public short getShort(int idx) {
        return data.getShort(idx);
    }

}
