package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.lsmtree.log.LogEntry;

import java.util.Iterator;

public interface EntryIterator<K extends Comparable<K>, V> extends Iterator<LogEntry<K, V>>, AutoCloseable {
}
