package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.lsmtree.sstable.Entry;

import java.util.Iterator;

public interface EntryIterator<K extends Comparable<K>, V> extends Iterator<Entry<K, V>>, AutoCloseable {
}
