package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.Index;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexAppender implements Index {

    private static final String INDEX_DIR = "index";
    private final LogAppender<IndexEntry> appender;

    public IndexAppender(File rootDir, int logSize, int numElements, boolean useCompression) {
        Codec codec = useCompression ? new SnappyCodec() : Codec.noCompression();
        File indexDirectory = new File(rootDir, INDEX_DIR);
        this.appender = LogAppender.builder(indexDirectory, new IndexEntrySerializer())
                .compactionStrategy(new IndexCompactor())
                .compactionThreshold(3)
                .segmentSize(logSize)
                .name("index-appender")
                .storageMode(StorageMode.MMAP)
//                .disableCompaction()
                .namingStrategy(new IndexNaming())
                .open(new IndexSegmentFactory(indexDirectory, numElements, codec));
    }


    @Override
    public LogIterator<IndexEntry> indexIterator(Direction direction) {
        return appender.iterator(direction);
    }

    //FIXME not releasing readers ??
    @Override
    public LogIterator<IndexEntry> indexIterator(Direction direction, Range range) {
        List<LogIterator<IndexEntry>> iterators = appender.streamSegments(direction)
                .map(seg -> (IndexSegment) seg)
                .map(idxSeg -> idxSeg.indexIterator(direction, range))
                .collect(Collectors.toList());

        return Iterators.concat(iterators);
    }

    @Override
    public Stream<IndexEntry> indexStream(Direction direction) {
        return Iterators.closeableStream(indexIterator(direction));
    }

    @Override
    public Stream<IndexEntry> indexStream(Direction direction, Range range) {
        return Iterators.closeableStream(indexIterator(direction, range));
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        LogIterator<Log<IndexEntry>> segments = appender.segments(Direction.BACKWARD);
        while (segments.hasNext()) {
            IndexSegment next = (IndexSegment) segments.next();
            Optional<IndexEntry> fromDisk = next.get(stream, version);
            if (fromDisk.isPresent()) {
                return fromDisk;
            }
        }
        return Optional.empty();
    }

    @Override
    public int version(long stream) {
        LogIterator<Log<IndexEntry>> segments = appender.segments(Direction.BACKWARD);
        while (segments.hasNext()) {
            IndexSegment segment = (IndexSegment) segments.next();
            int version = segment.version(stream);
            if (version >= 0) {
                return version;
            }
        }
        return IndexEntry.NO_VERSION;
    }

    public void compact() {
        appender.compact();
    }

    @Override
    public void close() {
        appender.close();
    }

    public void append(IndexEntry indexEntry) {
        appender.append(indexEntry);
    }

    public void roll() {
        appender.roll();
    }

    public long entries() {
        return appender.entries();
    }

    public List<Log<IndexEntry>> segments(){
        return Iterators.toList(appender.segments(Direction.FORWARD));
    }

    public PollingSubscriber<IndexEntry> poller() {
        return appender.poller();
    }

    public void flush() {
        appender.flush();
    }


    public static class IndexNaming extends ShortUUIDNamingStrategy {
        @Override
        public String prefix() {
            return "index-" + super.prefix();
        }
    }

    private static class IndexSegmentFactory implements SegmentFactory<IndexEntry> {

        private final File directory;
        private final int numElements;
        private final Codec codec;

        private IndexSegmentFactory(File directory, int numElements, Codec codec) {
            this.directory = directory;
            this.numElements = numElements;
            this.codec = codec;
        }

        @Override
        public IndexSegment createOrOpen(Storage storage, Serializer<IndexEntry> serializer, IDataStream reader, String magic, Type type) {
            return new IndexSegment(storage, reader, magic, type, directory, codec, numElements);
        }
    }

}
