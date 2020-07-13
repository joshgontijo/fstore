package io.joshworks.ilog.lsm;

import io.joshworks.ilog.RecordUtils;
import io.joshworks.ilog.record.Record;

public class LsmRecordUtils {

    static Record add(long key, String value) {
        return RecordUtils.create(key, value);
    }

    static Record delete(long key) {
        return RecordUtils.create(key, null);
    }
}
