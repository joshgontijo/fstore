package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public interface EventLogIterator extends LogIterator<EventRecord> {

    default TypeMatch when(String type, Consumer<EventRecord> handler) {
        return new TypeMatch(this, type, handler);
    }

//    static EventLogIterator empty() {
//        return new EmptyEventLogIterator();
//    }

    static EventLogIterator of(LogIterator<EventRecord> iterator) {
        return new DelegateIterator(iterator);
    }

    class TypeMatch {
        private final Map<String, Consumer<EventRecord>> matchers = new HashMap<>();
        private final EventLogIterator iterator;

        TypeMatch(EventLogIterator iterator, String type, Consumer<EventRecord> handler) {
            this.iterator = iterator;
            this.matchers.put(type, Objects.requireNonNull(handler));
        }

        public TypeMatch when(String type, Consumer<EventRecord> handler) {
            this.matchers.put(type, Objects.requireNonNull(handler));
            return this;
        }

        public void match() {
            iterator.stream().filter(er -> matchers.containsKey(er.type)).forEach(ev -> matchers.getOrDefault(ev.type, e -> {
            }).accept(ev));
        }
    }

//    class EmptyEventLogIterator implements EventLogIterator {
//
//        private final LogIterator<EventRecord> empty = Iterators.empty();
//
//        @Override
//        public long position() {
//            return empty.position();
//        }
//
//        @Override
//        public void close() throws IOException {
//            empty.close();
//        }
//
//        @Override
//        public boolean hasNext() {
//            return empty.hasNext();
//        }
//
//        @Override
//        public EventRecord next() {
//            return empty.next();
//        }
//    }

    class DelegateIterator implements EventLogIterator {

        private final LogIterator<EventRecord> delegate;

        public DelegateIterator(LogIterator<EventRecord> delegate) {
            this.delegate = delegate;
        }

        @Override
        public long position() {
            return delegate.position();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public EventRecord next() {
            return delegate.next();
        }
    }

}
