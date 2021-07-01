package io.joshworks.es2.log;

import io.joshworks.es2.LengthPrefixedChannelIterator;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.View;
import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

class TLogRestorer {

    private TLogRestorer() {

    }

    static void restore(View<SegmentChannel> view, Consumer<ByteBuffer> fn) {
        if (view.isEmpty()) { //no segments
            return;
        }

        var head = view.head();
        long flushPos = restoreHead(head);

        boolean hasFlushEvent = flushPos >= 0;
        processEntries(fn, head, hasFlushEvent ? flushPos : 0);
        if (hasFlushEvent) {
            return;//all non flushed entries have been processed
        }

        var revIt = view.reverse(); //from head backwards
        while (revIt.hasNext()) {
            var seg = revIt.next();
            if (seg == head) {
                continue; //we already processed head
            }
            flushPos = scan(seg);
            hasFlushEvent = flushPos >= 0;
            processEntries(fn, seg, hasFlushEvent ? flushPos : 0);
            if (hasFlushEvent) {
                return;//all non flushed entries have been processed
            }
        }
    }

    private static void processEntries(Consumer<ByteBuffer> fn, SegmentChannel head, long startPos) {
        var segIt = new LengthPrefixedChannelIterator(head, startPos);
        while (segIt.hasNext()) {
            var entry = segIt.next();
            if (Type.DATA == Type.of(entry)) {
                //without hader and footer
                ByteBuffer data = entry.slice(TLog.HEADER_SIZE, entry.remaining() - TLog.HEADER_SIZE - TLog.FOOTER_SIZE);
                fn.accept(data);
            }
        }
    }

    private static long scan(SegmentChannel seg) {
        long lastFlushEventPos = -1;
        var it = new LengthPrefixedChannelIterator(seg);
        while (it.hasNext()) {
            var next = it.next();
            if (Type.FLUSH == Type.of(next)) {
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
            var next = it.next();
            if (!isEntryValid(next)) {
                System.err.println("Found invalid entry at position " + logPos);
                head.resize(logPos);
            }
            if (Type.FLUSH == Type.of(next)) {
                lastFlushEventPos = it.position();
            }
        }
        return lastFlushEventPos;
    }

    private static boolean isEntryValid(ByteBuffer data) {
        if (data.remaining() < TLog.HEADER_SIZE + TLog.FOOTER_SIZE) {
            return false;
        }
        var ppos = data.position();
        try {
            var entrySize = data.getInt();
            var crc = data.getInt();
            var sequence = data.getLong();
            var type = data.get();

            if (entrySize < 0 || entrySize > data.capacity()) {
                return false;
            }
            if (sequence < 0) {
                return false;
            }
            if (Type.of(type) == null) {
                return false;
            }

            var dataSize = entrySize - TLog.HEADER_SIZE - TLog.FOOTER_SIZE;
            int computed = ByteBufferChecksum.crc32(data, data.position(), dataSize);
            return computed == crc;
        } finally {
            data.position(ppos);
        }
    }
}
