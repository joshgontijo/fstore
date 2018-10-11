package io.joshworks.fstore.lsm.sstable;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.segment.Log;

import java.io.IOException;

public class SSTables<K extends Comparable<K>, V> extends LogAppender<Entry<K, V>>{

    public SSTables(Config<Entry<K, V>> config, SegmentFactory<Entry<K, V>> factory) {
        super(config, factory);
    }

    public V getByKey(K key) {
        try (LogIterator<Log<Entry<K, V>>> segments = segments(Direction.BACKWARD)) {
            while(segments.hasNext()) {
                SSTable<K, V> sstable = (SSTable<K, V>) segments.next();
                V found = sstable.get(key);
                if(found != null) {
                    return found;
                }
            }
            return null;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
