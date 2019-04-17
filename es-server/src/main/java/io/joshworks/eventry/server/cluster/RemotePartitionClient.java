package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.network.ClusterNode;
import io.joshworks.eventry.network.client.ClusterClient;
import io.joshworks.eventry.server.cluster.messages.Append;
import io.joshworks.eventry.server.cluster.messages.AppendResult;
import io.joshworks.eventry.server.cluster.messages.EventBatch;
import io.joshworks.eventry.server.cluster.messages.EventData;
import io.joshworks.eventry.server.cluster.messages.FromAll;
import io.joshworks.eventry.server.cluster.messages.FromStream;
import io.joshworks.eventry.server.cluster.messages.Get;
import io.joshworks.eventry.server.cluster.messages.IteratorClose;
import io.joshworks.eventry.server.cluster.messages.IteratorCreated;
import io.joshworks.eventry.server.cluster.messages.IteratorNext;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import org.jgroups.Address;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static io.joshworks.eventry.log.EventRecord.NO_EXPECTED_VERSION;
import static io.joshworks.eventry.server.cluster.RemoteIterators.DEFAULT_BATCH_SIZE;
import static io.joshworks.eventry.server.cluster.RemoteIterators.DEFAULT_TIMEOUT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;

//CLIENT
public class RemotePartitionClient implements IEventStore {

    private final ClusterClient client;
    private final int partitionId;
    private final ClusterNode node;

    public RemotePartitionClient(ClusterNode node, int partitionId, ClusterClient client) {
        this.client = client;
        this.node = node;
        this.partitionId = partitionId;
    }

    @Override
    public void compact() {

    }

    @Override
    public void close() {

    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        return null;
    }

    @Override
    public EventRecord linkTo(String dstStream, StreamName source, String sourceType) {
        return null;
    }

    @Override
    public EventRecord append(EventRecord event) {
        return append(event, NO_EXPECTED_VERSION);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        Append append = new Append(event, expectedVersion);
        AppendResult response = client.send(node.address, append);
        return new EventRecord(event.stream, event.type, response.version, response.timestamp, event.body, event.metadata);
    }

    @Override
    public EventLogIterator fromStream(StreamName stream) {
        IteratorCreated it = client.send(node.address, new FromStream(stream.toString(), DEFAULT_TIMEOUT, DEFAULT_BATCH_SIZE));
        return new RemoteStoreClientIterator(client, node.address, it.iteratorId);
    }

    @Override
    public EventLogIterator fromStreams(String streamPattern, boolean ordered) {
        return null;
    }

    @Override
    public EventLogIterator fromStreams(Set<StreamName> streams, boolean ordered) {
        return null;
    }

    @Override
    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return fromAll(linkToPolicy, systemEventPolicy, null);
    }

    @Override
    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
        IteratorCreated it = client.send(node.address, new FromAll(DEFAULT_TIMEOUT, DEFAULT_BATCH_SIZE, partitionId, linkToPolicy, systemEventPolicy, lastEvent));
        return new RemoteStoreClientIterator(client, node.address, it.iteratorId);
    }

    @Override
    public void createStream(String name) {
        createStream(name, NO_MAX_COUNT, NO_MAX_AGE);
    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {
        createStream(name, NO_MAX_COUNT, NO_MAX_AGE, new HashMap<>(), new HashMap<>());
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return null;
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        return Optional.empty();
    }

    @Override
    public void truncate(String stream, int version) {

    }

    @Override
    public EventRecord get(StreamName streamName) {
        EventData eventData = client.send(node.address, new Get(streamName));
        return eventData.record;
    }

    @Override
    public int version(String stream) {
        return 0;
    }

    private static class RemoteStoreClientIterator implements EventLogIterator {

        private final ClusterClient client;
        private final Address address;
        private final String iteratorId;

        private final Queue<EventRecord> cached = new ArrayDeque<>();

        private RemoteStoreClientIterator(ClusterClient client, Address address, String iteratorId) {
            this.client = client;
            this.iteratorId = iteratorId;
            this.address = address;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public void close() {
            client.send(address, new IteratorClose(iteratorId));
        }

        @Override
        public boolean hasNext() {
            if (!cached.isEmpty()) {
                return true;
            }
            fetch();
            return !cached.isEmpty();
        }

        @Override
        public EventRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No remote element found");
            }
            return cached.poll();
        }

        private void fetch() {
            EventBatch events = client.send(address, new IteratorNext(iteratorId));
            cached.addAll(events.records);
        }

    }

}