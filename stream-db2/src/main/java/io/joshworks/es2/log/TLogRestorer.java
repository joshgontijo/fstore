package io.joshworks.es2.log;

import io.joshworks.es2.Event;
import io.joshworks.es2.LengthPrefixedChannelIterator;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.View;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static io.joshworks.es2.EventStore.isFlushEvent;
import static io.joshworks.es2.log.TLog.NO_SEQUENCE;

class TLogRestorer {

    private TLogRestorer() {

    }

    /**
     * Scan the logs rom the last flush and validate all entries, returns the last valid sequence in the log or -1 if empty
     */
    static long restore(View<SegmentChannel> view, Consumer<ByteBuffer> fn) {


        long lastSequence = NO_SEQUENCE;

        if (view.isEmpty()) { //no segments
            return lastSequence;
        }

        var head = view.head();

        long flushPos = restoreHead(head);
        boolean hasFlushEvent = flushPos >= 0;

        lastSequence = processEntries(fn, head, hasFlushEvent ? flushPos : 0);
        if (hasFlushEvent) {
            return lastSequence;//all non flushed entries have been processed
        }

        var revIt = view.reverse(); //from head backwards
        while (revIt.hasNext()) {
            var seg = revIt.next();
            if (seg == head) {
                continue; //we already processed head
            }
            flushPos = lastFlushPos(seg);
            hasFlushEvent = flushPos >= 0;
            processEntries(fn, seg, hasFlushEvent ? flushPos : 0); //don't use lastSequence return here as they are always lower
            if (hasFlushEvent) {
                break;//all non flushed entries have been processed
            }
        }

        return lastSequence;
    }

    private static long processEntries(Consumer<ByteBuffer> fn, SegmentChannel head, long startPos) {
        var segIt = new LengthPrefixedChannelIterator(head, startPos);
        long lastSequence = NO_SEQUENCE;
        while (segIt.hasNext()) {
            var entry = segIt.next();
            lastSequence = Event.sequence(entry);
            fn.accept(entry);
        }
        return lastSequence;
    }

    private static long lastFlushPos(SegmentChannel seg) {
        long lastFlushEventPos = -1;
        var it = new LengthPrefixedChannelIterator(seg);
        while (it.hasNext()) {
            var entry = it.next();
            if (isFlushEvent(entry)) {
                lastFlushEventPos = it.position();
            }
        }
        return lastFlushEventPos;
    }

    private static long restoreHead(SegmentChannel head) {
        //forward iterator to validate entries
        long lastFlushEventPos = -1;
        var it = new LengthPrefixedChannelIterator(head);
        while (it.hasNext()) {
            var logPos = it.position();
            var entry = it.next();
            if (!Event.isValid(entry)) {
                System.err.println("Found invalid entry at position " + logPos);
                head.resize(logPos);
            }
            if (isFlushEvent(entry)) {
                lastFlushEventPos = it.position();
            }
        }
        return lastFlushEventPos;
    }
}
