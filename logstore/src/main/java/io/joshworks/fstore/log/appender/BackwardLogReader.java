package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class BackwardLogReader<T> implements LogIterator<T> {

    private final Iterator<LogIterator<T>> segmentsIterators;
    private LogIterator<T> current;
    private int segmentIdx;

    BackwardLogReader(long startPosition, Levels<T> levels) {
        this.segmentsIterators = levels.apply(Direction.BACKWARD, segs -> {
            int numSegments = segs.size();
            int segIdx = LogAppender.getSegment(startPosition);

            this.segmentIdx = numSegments - (numSegments - segIdx);
            int skips = (numSegments - 1) - segIdx;

            LogAppender.validateSegmentIdx(segmentIdx, startPosition, levels);
            long positionOnSegment = LogAppender.getPositionOnSegment(startPosition);


            LogIterator<Log<T>> segments = Iterators.of(segs);

            // skip
            for (int i = 0; i < skips; i++) {
                segments.next();
            }

            if (segments.hasNext()) {
                this.current = segments.next().iterator(positionOnSegment, Direction.BACKWARD);
            }

            List<LogIterator<T>> subsequentIterators = new ArrayList<>();
            while (segments.hasNext()) {
                subsequentIterators.add(segments.next().iterator(Direction.BACKWARD));
            }
            return subsequentIterators.iterator();
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
        return current.hasNext() || segmentsIterators.hasNext();
    }

    @Override
    public T next() {
        if ((current == null || !current.hasNext()) && segmentsIterators.hasNext()) {
            IOUtils.closeQuietly(current);
            current = segmentsIterators.next();
            segmentIdx--;
        }
        if (current == null || !hasNext()) {
            return null;
        }
        return current.next();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(current);
        segmentsIterators.forEachRemaining(IOUtils::closeQuietly);
    }
}
