package io.joshworks.fstore.log;

public interface SegmentIterator<T> extends LogIterator<T> {

    boolean endOfLog();

}
