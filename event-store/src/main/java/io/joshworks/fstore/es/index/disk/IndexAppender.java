package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.Index;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.Order;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.segment.block.FixedSizeBlockSerializer;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexAppender extends LogAppender<IndexEntry, IndexSegment> implements Index {

    public IndexAppender(Config<IndexEntry> config, int numElements, boolean compress) {
        super(config, new IndexSegmentFactory(config.directory, numElements, compress));
    }

    //FIXME not releasing readers
    @Override
    public LogIterator<IndexEntry> iterator(Range range) {
        List<LogIterator<IndexEntry>> iterators = streamSegments(Order.FORWARD)
                .map(idxSeg -> idxSeg.iterator(range))
                .collect(Collectors.toList());

        return Iterators.concat(iterators);
    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        return Iterators.stream(iterator(range));
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Iterator<IndexSegment> segments = segments(Order.BACKWARD);
        while (segments.hasNext()) {
            IndexSegment next = segments.next();
            Optional<IndexEntry> fromDisk = next.get(stream, version);
            if (fromDisk.isPresent()) {
                return fromDisk;
            }
        }
        return Optional.empty();
    }

    @Override
    public int version(long stream) {
        LogIterator<IndexSegment> segments = segments(Order.BACKWARD);
        while (segments.hasNext()) {
            IndexSegment segment = segments.next();
            int version = segment.version(stream);
            if (version >= 0) {
                return version;
            }
        }
        return IndexEntry.NO_VERSION;
    }

    @Override
    public LogIterator<IndexEntry> iterator() {
        List<LogIterator<IndexEntry>> segments = streamSegments(Order.FORWARD).map(Log::iterator).collect(Collectors.toList());
        return Iterators.concat(segments);
    }

    public static class IndexNaming extends ShortUUIDNamingStrategy {
        @Override
        public String prefix() {
            return "index-" + super.prefix();
        }
    }

    private static class IndexSegmentFactory implements SegmentFactory<IndexEntry, IndexSegment> {

        private final File directory;
        private final int numElements;
        private final boolean compress;

        private IndexSegmentFactory(File directory, int numElements, boolean compress) {
            this.directory = directory;
            this.numElements = numElements;
            this.compress = compress;
        }

        @Override
        public IndexSegment createOrOpen(Storage storage, Serializer<IndexEntry> serializer, DataReader reader, String magic, Type type) {
            return new IndexSegment(storage, new FixedSizeBlockSerializer<>(serializer, IndexEntry.BYTES, compress), reader, magic, type, directory, numElements);
        }
    }

}
