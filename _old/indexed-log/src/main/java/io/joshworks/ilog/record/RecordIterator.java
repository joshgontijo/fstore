//package io.joshworks.ilog.record;
//
//import io.joshworks.fstore.core.RuntimeIOException;
//import io.joshworks.fstore.core.util.Iterators;
//
//public class RecordIterator implements Iterators.CloseableIterator<Record> {
//
//    private final Iterators.CloseableIterator<Record> delegate;
//    private boolean hasPeeked;
//    private Record peekedElement;
//
//    public RecordIterator(Iterators.CloseableIterator<Record> delegate) {
//        this.delegate = delegate;
//    }
//
//    @Override
//    public boolean hasNext() {
//        return hasPeeked || delegate.hasNext();
//    }
//
//    @Override
//    public Record next() {
//        if (!hasPeeked) {
//            return delegate.next();
//        }
//        Record result = peekedElement;
//        peekedElement = null;
//        hasPeeked = false;
//        return result;
//    }
//
//    public Record peek() {
//        if (!hasPeeked) {
//            peekedElement = delegate.next();
//            hasPeeked = true;
//        }
//        return peekedElement;
//    }
//
//    @Override
//    public void close() {
//        try {
//            delegate.close();
//        } catch (Exception e) {
//            throw new RuntimeIOException(e);
//        }
//
//    }
//}