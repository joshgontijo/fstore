package io.joshworks.eventry.log;

import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.index.TableIndex;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;
import java.util.Optional;

public class RecordCleanup implements SegmentCombiner<EventRecord> {

    private final Streams streams;

    public RecordCleanup(Streams streams) {
        this.streams = streams;
    }

    @Override
    public void merge(List<? extends Log<EventRecord>> segments, Log<EventRecord> output) {
        if (segments.size() != 1) {
            throw new IllegalArgumentException("Cleanup task can only be executed on a single segment at once");
        }

        Log<EventRecord> log = segments.get(0);

        try (LogIterator<EventRecord> iterator = log.iterator(Direction.FORWARD)) {

            while (iterator.hasNext()) {
                long oldPosition = iterator.position();

                EventRecord record = iterator.next();

                StreamMetadata metadata = getMetadata(record.stream);

                int version = record.version;
                long timestamp = record.timestamp;

                if (skipEntry(metadata, version, timestamp)) {
                    continue;
                }

                if (record.isLinkToEvent() && skipLinkToEntry(record, timestamp)) {
                    continue;
                }

                output.append(record);

                //TODO add negative position to IndexEntry that should be removed
                //TODO add logic to IndexCompactor to remove those entries


                //TODO add position mapping to footer
                //TODO New Segment class for the EventLog is needed to handle the mapping on read
                //TODO mapping should be relative offset that the deleted entry adds to the subsequent entries

            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private boolean skipLinkToEntry(EventRecord record, long timestamp) {
        LinkTo linkTo = LinkTo.from(record);
        String targetStream = linkTo.stream;
        int targetVersion = linkTo.version;

        StreamMetadata tgtMetadata = getMetadata(targetStream);

        //isExpired we can use the LinkTo event TS, since it will always be equals or greater than the original TS
        return skipEntry(tgtMetadata, targetVersion, timestamp);
    }

    private boolean skipEntry(StreamMetadata metadata, int version, long timestamp) {
        return isExpired(timestamp, metadata) || isObsolete(version, metadata) || isTruncatedEntry(version, metadata) || isStreamDeleted(metadata);
    }

    private StreamMetadata getMetadata(String stream) {
        Optional<StreamMetadata> metadataOpt = streams.get(stream);
        if (!metadataOpt.isPresent()) {
            //TODO replace with a log warn
            throw new RuntimeException("No metadata available for stream: " + stream);
        }
        return metadataOpt.get();
    }

    private boolean isTruncatedEntry(int recordVersion, StreamMetadata metadata) {
        return metadata.truncated() && recordVersion <= metadata.truncated;
    }

    private boolean isObsolete(int recordVersion, StreamMetadata metadata) {
        int currentStreamVersion = streams.version(metadata.hash);
        return metadata.maxCount > 0 && currentStreamVersion - recordVersion >= metadata.maxCount;
    }

    private boolean isExpired(long recordTimestamp, StreamMetadata metadata) {
        return metadata.maxAge > 0 && System.currentTimeMillis() - recordTimestamp > metadata.maxAge;
    }

    private boolean isStreamDeleted(StreamMetadata metadata) {
        return metadata.streamDeleted();
    }
}
