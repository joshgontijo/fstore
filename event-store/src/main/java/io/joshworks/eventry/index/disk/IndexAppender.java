package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.MemIndex;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.headers.Type;

import java.io.Closeable;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;

public class IndexAppender implements Closeable {

    private static final String INDEX_DIR = "index";
    private static final String STORE_NAME = "index";
    private final LogAppender<IndexEntry> appender;

    public IndexAppender(File rootDir, Function<Long, StreamMetadata> streamSupplier, int logSize, int numElements, boolean useCompression) {
        Codec codec = useCompression ? new SnappyCodec() : Codec.noCompression();
        File indexDirectory = new File(rootDir, INDEX_DIR);
        this.appender = LogAppender.builder(indexDirectory, new IndexEntrySerializer())
                .compactionStrategy(new IndexCompactor(streamSupplier))
                .compactionThreshold(3)
                .segmentSize(logSize)
                .disableAutoRoll()
                .name(STORE_NAME)
                .flushMode(FlushMode.ON_ROLL)
                .storageMode(StorageMode.MMAP)
                .namingStrategy(new IndexNaming())
                .open(new IndexSegmentFactory(indexDirectory, numElements, codec));
    }

    public LogIterator<IndexEntry> indexedIterator(Direction direction, Range range) {
        return appender.applyToSegments(direction, segments -> {
            List<LogIterator<IndexEntry>> iterators = Iterators.stream(segments)
                    .map(seg -> (IndexSegment) seg)
                    .map(idxSeg -> idxSeg.indexedIterator(direction, range))
                    .collect(Collectors.toList());
            return Iterators.concat(iterators);
        });
    }

    public LogIterator<IndexEntry> iterator(Direction direction) {
        return appender.iterator(direction);
    }

    public LogIterator<IndexEntry> iterator(Direction direction, long position) {
        return appender.iterator(direction, position);
    }

    public Optional<IndexEntry> get(long stream, int version) {
        //always backward
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<IndexEntry> segment : segments) {
                IndexSegment indexSegment = (IndexSegment) segment;
                Optional<IndexEntry> fromDisk = indexSegment.get(stream, version);
                if (fromDisk.isPresent()) {
                    return fromDisk;
                }
            }
            return Optional.empty();
        });
    }

    public List<IndexEntry> getBlockEntries(long stream, int version) {
        //always backward
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<IndexEntry> segment : segments) {
                IndexSegment indexSegment = (IndexSegment) segment;
                if(segment.readOnly()) { //only query completed segments
                    List<IndexEntry> indexEntries = indexSegment.readBlockEntries(stream, version);
                    if (!indexEntries.isEmpty()) {
                        return indexEntries;
                    }
                }
            }
            return Collections.emptyList();
        });
    }

    public int version(long stream) {
        //always backward
        return appender.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<IndexEntry> segment : segments) {
                IndexSegment indexSegment = (IndexSegment) segment;
                int version = indexSegment.lastVersionOf(stream);
                if (version >= 0) {
                    return version;
                }
            }
            return NO_VERSION;
        });
    }

    public void compact() {
        appender.compact();
    }

    public void close() {
        appender.close();
    }

    //this method must ensure that, all the memtable entries are stored in the same segment file
    public synchronized void writeToDisk(MemIndex memIndex) {
        memIndex.iterator().forEachRemaining(appender::append);
        appender.roll();
    }

    public long entries() {
        return appender.entries();
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
