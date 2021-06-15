package io.joshworks.fstore.log;

import io.joshworks.fstore.es.shared.LinkTo;
import io.joshworks.fstore.index.Index;
import io.joshworks.fstore.stream.StreamMetadata;
import io.joshworks.fstore.stream.Streams;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;

import static io.joshworks.fstore.EventUtils.isValidEntry;

public class RecordCleanup implements SegmentCombiner<EventRecord> {

    private final Streams streams;
    private final Index index;

    public RecordCleanup(Streams streams, Index index) {
        this.streams = streams;
        this.index = index;
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

                if (!isValidEntry(metadata, version, timestamp, index::version)) {
                    continue;
                }

                if (record.isLinkToEvent() && !validLinkToEntry(record, timestamp)) {
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

    private boolean validLinkToEntry(EventRecord record, long timestamp) {
        LinkTo linkTo = LinkTo.from(record);
        String targetStream = linkTo.stream;
        int targetVersion = linkTo.version;

        StreamMetadata tgtMetadata = getMetadata(targetStream);

        //isExpired we can use the LinkTo event TS, since it will always be equals or greater than the original TS
        return isValidEntry(tgtMetadata, targetVersion, timestamp, index::version);
    }

    private StreamMetadata getMetadata(String stream) {
        StreamMetadata metadataOpt = streams.get(stream);
        if (metadataOpt == null) {
            //TODO replace with a log warn
            throw new RuntimeException("No metadata available for stream: " + stream);
        }
        return metadataOpt;
    }

}
