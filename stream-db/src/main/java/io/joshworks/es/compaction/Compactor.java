package io.joshworks.es.compaction;

import io.joshworks.es.Event;
import io.joshworks.es.log.Log;
import io.joshworks.es.log.LogSegment;
import io.joshworks.fstore.core.util.Threads;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Compactor {

    private final ExecutorService executor = Executors.newSingleThreadExecutor(Threads.namedThreadFactory("compactor"));

    public void compactLog(Log log, int threshold) {
        for (int level = 0; level <= log.depth(); level++) {
            List<LogSegment> segments = log.compactionSegments(level);
            if (segments.size() < threshold) {
                return;
            }

            List<List<LogSegment>> chunks = batches(segments, threshold)
                    .filter(l -> l.size() >= threshold)
                    .collect(Collectors.toList());

            for (List<LogSegment> chunk : chunks) {
                LogSegment output = outputSegment(log, chunk);

                List<SortingSegment> sortedSegments = chunk.stream().map(SortingSegment::sort).collect(Collectors.toList());

                List<PeekingIterator<ByteBuffer>> segIterators = sortedSegments.stream()
                        .map(seg -> new PeekingIterator<>(seg.iterator()))
                        .collect(Collectors.toList());

                //TODO create sink, add a buffered layer and pass it here
                UniqueMergeCombiner combiner = new UniqueMergeCombiner(segIterators, Event::compare, output::append);
                combiner.merge();

                sortedSegments.forEach(SortingSegment::delete);

                output.flush();
                log.merge(output, chunk);

            }
        }
    }

    public LogSegment outputSegment(Log log, List<LogSegment> chunk) {
        File outFile = log.newMergeFile(chunk);
        long size = chunk.stream().mapToLong(LogSegment::size).sum();
        return LogSegment.create(outFile, size);
    }


    public static <T> Stream<List<T>> batches(List<T> source, int length) {
        if (length <= 0)
            throw new IllegalArgumentException("length = " + length);
        int size = source.size();
        if (size <= 0)
            return Stream.empty();
        int fullChunks = (size - 1) / length;
        return IntStream.range(0, fullChunks + 1).mapToObj(
                n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
    }

}
