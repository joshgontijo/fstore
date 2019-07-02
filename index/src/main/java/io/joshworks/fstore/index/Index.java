package io.joshworks.fstore.index;

import io.joshworks.fstore.log.segment.footer.FooterWriter;

public interface Index<K extends Comparable<K>> extends Iterable<IndexEntry<K>> {

    long get(K key);

    void add(K key, long position);

    void writeTo(FooterWriter writer);

}
