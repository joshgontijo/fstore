package io.joshworks.fstore.index;

import io.joshworks.fstore.log.segment.footer.FooterWriter;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface Index<K extends Comparable<K>> extends Iterable<IndexEntry<K>> {

    long get(K key);

    void add(K key, long position);

    void writeTo(Consumer<ByteBuffer> writer);

}
