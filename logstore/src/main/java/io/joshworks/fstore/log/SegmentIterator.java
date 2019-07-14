package io.joshworks.fstore.log;

public interface SegmentIterator<T> extends LogIterator<T> {

    boolean endOfLog();

    static <T> SegmentIterator<T> empty() {
        return new SegmentIterator<>() {
            @Override
            public boolean endOfLog() {
                return true;
            }

            @Override
            public long position() {
                return -1;
            }

            @Override
            public void close() {

            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                return null;
            }
        };
    }

}
