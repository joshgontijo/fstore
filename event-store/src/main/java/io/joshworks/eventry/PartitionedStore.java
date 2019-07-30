//package io.joshworks.eventry;
//
//import io.joshworks.eventry.log.EventRecord;
//import io.joshworks.eventry.partition.Partition;
//import io.joshworks.eventry.partition.Partitions;
//import io.joshworks.eventry.stream.StreamInfo;
//import io.joshworks.eventry.stream.StreamMetadata;
//import io.joshworks.fstore.log.iterators.Iterators;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Queue;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//public class PartitionedStore implements IEventStore {
//
//    private final Partitions partitions;
//    private final File root;
//
//    public PartitionedStore(Partitions partitions, File root) {
//        this.partitions = partitions;
//        this.root = root;
//    }
//
//    @Override
//    public void compact() {
//        partitions.all().stream().map(Partition::store).forEach(IEventStore::compact);
//    }
//
//    @Override
//    public void close() {
//        partitions.all().forEach(Partition::close);
//    }
//
//    @Override
//    public EventRecord linkTo(String stream, EventRecord event) {
//        return null;
//    }
//
//    @Override
//    public EventRecord linkTo(String dstStream, StreamName source, String sourceType) {
//        return null;
//    }
//
//    @Override
//    public EventRecord append(EventRecord event) {
//        return partitions.select(event.stream).store().append(event);
//    }
//
//    @Override
//    public EventRecord append(EventRecord event, int expectedVersion) {
//        return partitions.select(event.stream).store().append(event, expectedVersion);
//    }
//
//    @Override
//    public EventLogIterator fromStream(StreamName stream) {
//        return partitions.select(stream.name()).store().fromStream(stream);
//    }
//
//    @Override
//    public EventLogIterator fromStreams(String streamPattern) {
//        //TODO implement properly
//        List<EventLogIterator> iterators = partitions.all().stream()
//                .map(Partition::store)
//                .map(s -> s.fromStreams(streamPattern))
//                .collect(Collectors.toList());
//
//        return EventLogIterator.of(Iterators.concat(iterators));
//    }
//
//    @Override
//    public EventLogIterator fromStreams(Set<StreamName> streams) {
//        //TODO implement properly
//        Map<Partition, Set<StreamName>> grouped = streams.stream()
//                .map(s -> new StreamPartition(s, partitions.select(s.name())))
//                .collect(Collectors.groupingBy(a -> a.partition, Collectors.mapping(b -> b.streamName, Collectors.toSet())));
//
//        List<EventLogIterator> iterators = grouped.entrySet()
//                .stream()
//                .map(kv -> kv.getKey().store().fromStreams(kv.getValue()))
//                .collect(Collectors.toList());
//
//        return EventLogIterator.of(Iterators.concat(iterators));
//    }
//
//    @Override
//    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
//        return null;
//    }
//
//    @Override
//    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
//        return null;
//    }
//
//    @Override
//    public void createStream(String name) {
//
//    }
//
//    @Override
//    public void createStream(String name, int maxCount, long maxAge) {
//
//    }
//
//    @Override
//    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
//        return null;
//    }
//
//    @Override
//    public List<StreamInfo> streamsMetadata() {
//        return null;
//    }
//
//    @Override
//    public Optional<StreamInfo> streamMetadata(String stream) {
//        return Optional.empty();
//    }
//
//    @Override
//    public void truncate(String stream, int fromVersion) {
//
//    }
//
//    @Override
//    public EventRecord get(StreamName stream) {
//        return null;
//    }
//
//    @Override
//    public int version(String stream) {
//        return 0;
//    }
//
//    @Override
//    public int count(String stream) {
//        return 0;
//    }
//
//
//    private static class StreamPartition {
//        private final StreamName streamName;
//        private final Partition partition;
//
//        private StreamPartition(StreamName streamName, Partition partition) {
//            this.streamName = streamName;
//            this.partition = partition;
//        }
//    }
//
//    //TODO this should have N items of each partition (batched)
//    private static final class PartitionedIterator implements EventLogIterator {
//
//        private final Map<String, EventLogIterator> iterators;
//
//        private PartitionedIterator(Map<String, EventLogIterator> iterators) {
//            this.iterators = iterators;
//        }
//
//        @Override
//        public boolean hasNext() {
//            for (Map.Entry<String, EventLogIterator> kv : iterators.entrySet()) {
//                if (kv.getValue().hasNext()) {
//                    return true;
//                }
//            }
//            return false;
//        }
//
//        @Override
//        public EventRecord next() {
//            for (Map.Entry<String, EventLogIterator> kv : iterators.entrySet()) {
//                if (kv.getValue().hasNext()) {
//
//                }
//            }
//        }
//
//        @Override
//        public long position() {
//            return 0;
//        }
//
//        @Override
//        public void close() throws IOException {
//
//        }
//
//    }
//}
