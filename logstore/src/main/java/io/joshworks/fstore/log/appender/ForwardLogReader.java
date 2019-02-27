package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;

//FORWARD SCAN: When at the end of a segment, advance to the next, so the current position is correct
//----
//BACKWARD SCAN: At the beginning of a segment, do not move to previous until next is called.
// hasNext calls will always return true, since the previous segment always has data
class ForwardLogReader<T> implements LogIterator<T> {

    private final Queue<SegmentIterator<T>> segmentsQueue = new ArrayDeque<>();
    private final Consumer<ForwardLogReader<T>> closeListener;
    private SegmentIterator<T> current;
    private int segmentIdx;

    ForwardLogReader(long startPosition, Levels<T> levels, Consumer<ForwardLogReader<T>> closeListener) {
        this.closeListener = closeListener;
        levels.apply(Direction.FORWARD, segs -> {
            this.segmentIdx = LogAppender.getSegment(startPosition);

            LogAppender.validateSegmentIdx(segmentIdx, startPosition, levels);
            long positionOnSegment = LogAppender.getPositionOnSegment(startPosition);


            LogIterator<Log<T>> segments = Iterators.of(segs);
            // skip
            for (int i = 0; i < this.segmentIdx; i++) {
                segments.next();
            }

            if (segments.hasNext()) {
                this.current = segments.next().iterator(positionOnSegment, Direction.FORWARD);
            }

            while (segments.hasNext()) {
                this.segmentsQueue.add(segments.next().iterator(Direction.FORWARD));
            }

            return segmentsQueue;
        });

    }

    @Override
    public long position() {
        return LogAppender.toSegmentedPosition(segmentIdx, current.position());
    }

    @Override
    public boolean hasNext() {
        if (current == null) {
            return false;
        }
        if (current.endOfLog()) {
            IOUtils.closeQuietly(current);
            if (segmentsQueue.isEmpty()) {
                return false;
            }
            current = segmentsQueue.poll();
            segmentIdx++;
        }
        return current.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            return null;
        }
        T next = current.next();
        if (next == null && current.endOfLog()) {
            IOUtils.closeQuietly(current);
            if (!segmentsQueue.isEmpty()) {
                current = segmentsQueue.poll();
                segmentIdx++;
            }
        }
        return next;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(current);
        segmentsQueue.forEach(IOUtils::closeQuietly);
        segmentsQueue.clear();
        closeListener.accept(this);
    }

    void addSegment(Log<T> segment) {
        segmentsQueue.add(segment.iterator(Direction.FORWARD));
    }
}
