package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;

import java.nio.ByteBuffer;
import java.util.List;

public interface Block<T> {

    boolean add(T data);

    ByteBuffer pack(Codec codec);

    int entryCount();

    List<T> entries();

    T get(int pos);

    T first();

    T last();

    boolean readOnly();

    boolean isEmpty();

    List<Integer> entriesLength();


}
