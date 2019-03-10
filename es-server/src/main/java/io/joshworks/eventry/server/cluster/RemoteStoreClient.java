package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.State;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.TaskStatus;
import io.joshworks.eventry.server.cluster.client.ClusterClient;
import io.joshworks.eventry.server.cluster.messages.Append;
import io.joshworks.eventry.server.cluster.messages.AppendSuccess;
import io.joshworks.eventry.server.cluster.messages.EventData;
import io.joshworks.eventry.server.cluster.messages.FromAll;
import io.joshworks.eventry.server.cluster.messages.IteratorClose;
import io.joshworks.eventry.server.cluster.messages.IteratorCreated;
import io.joshworks.eventry.server.cluster.messages.IteratorNext;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.LogIterator;
import org.jgroups.Address;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

//CLIENT
public class RemoteStoreClient implements IEventStore {

    private final ClusterClient client;
    private final Address address;
    private final int partitionId;

    public RemoteStoreClient(ClusterClient client, Address address, int partitionId) {
        this.client = client;
        this.address = address;
        this.partitionId = partitionId;
    }

    @Override
    public void compact() {

    }

    @Override
    public State query(Set<String> streams, State state, String script) {
        return null;
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
        return null;
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
//        Append append = new Append(event, expectedVersion);
//        AppendSuccess response = client.send(address, append).as(AppendSuccess::new);
        return null; //TODO
    }

    @Override
    public EventRecord appendSystemEvent(EventRecord event) {
        return null;
    }

    @Override
    public EventLogIterator fromStream(StreamName stream) {
        return null;
    }

    @Override
    public EventLogIterator fromStreams(String streamPattern) {
        return null;
    }

    @Override
    public EventLogIterator fromStreams(Set<StreamName> streams) {
        return null;
    }

    @Override
    public LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return fromAll(linkToPolicy, systemEventPolicy, null);
    }

    @Override
    public LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
//        IteratorCreated it = client.send(address, new FromAll(10000, 20, partitionId, linkToPolicy, systemEventPolicy, lastEvent)).as(IteratorCreated::new);
//        return new RemoteStoreClientIterator(client, address, it.iteratorId);
        return null;
    }

    @Override
    public Collection<Projection> projections() {
        return null;
    }

    @Override
    public Projection projection(String name) {
        return null;
    }

    @Override
    public Projection createProjection(String script) {
        return null;
    }

    @Override
    public Projection updateProjection(String name, String script) {
        return null;
    }

    @Override
    public void deleteProjection(String name) {

    }

    @Override
    public void runProjection(String name) {

    }

    @Override
    public void resetProjection(String name) {

    }

    @Override
    public void stopProjectionExecution(String name) {

    }

    @Override
    public void disableProjection(String name) {

    }

    @Override
    public void enableProjection(String name) {

    }

    @Override
    public Map<String, TaskStatus> projectionExecutionStatus(String name) {
        return null;
    }

    @Override
    public Collection<Metrics> projectionExecutionStatuses() {
        return null;
    }

    @Override
    public void createStream(String name) {

    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {

    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        return null;
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
    public EventRecord get(StreamName stream) {
        return null;
    }

    @Override
    public EventRecord get(IndexEntry entry) {
        return null;
    }

    @Override
    public EventRecord resolve(EventRecord record) {
        return null;
    }

    @Override
    public int version(String stream) {
        return 0;
    }

    private static class RemoteStoreClientIterator implements LogIterator<EventRecord> {

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
            return cached.isEmpty();
        }

        @Override
        public EventRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No remote element found");
            }
            return cached.poll();
        }

        private void fetch() {
//            EventData eventData = client.send(address, new IteratorNext(iteratorId)).as(EventData::new);
//            cached.add(eventData.record);
        }

    }

}
