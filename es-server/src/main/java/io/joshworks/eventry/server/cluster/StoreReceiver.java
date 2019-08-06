package io.joshworks.eventry.server.cluster;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.network.ClusterMessage;
import io.joshworks.eventry.server.cluster.messages.Append;
import io.joshworks.eventry.server.cluster.messages.AppendResult;
import io.joshworks.eventry.server.cluster.messages.EventBatch;
import io.joshworks.eventry.server.cluster.messages.EventData;
import io.joshworks.eventry.server.cluster.messages.FromAll;
import io.joshworks.eventry.server.cluster.messages.FromStream;
import io.joshworks.eventry.server.cluster.messages.FromStreams;
import io.joshworks.eventry.server.cluster.messages.Get;
import io.joshworks.eventry.server.cluster.messages.IteratorCreated;
import io.joshworks.eventry.server.cluster.messages.IteratorNext;

import java.io.Closeable;
import java.util.List;

public class StoreReceiver implements Closeable {

    private final RemoteIterators remoteIterators = new RemoteIterators();
    private final IEventStore store;

    public StoreReceiver(IEventStore store) {
        this.store = store;
    }

    AppendResult append(Append append) {
        EventRecord created = store.append(append.event, append.expectedVersion);
        return new AppendResult(true, created.timestamp, created.version);
    }

    ClusterMessage get(Get get) {
        EventId eventId = EventId.parse(get.streamName);
        EventRecord eventRecord = store.get(eventId);
        return new EventData(eventRecord);
    }

    ClusterMessage fromAll(FromAll fromAll) {
        EventStoreIterator iterator = store.fromAll(fromAll.linkToPolicy, fromAll.systemEventPolicy);
        String iteratorId = remoteIterators.add(fromAll.timeout, fromAll.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    ClusterMessage fromStream(FromStream fromStream) {
        EventId eventId = EventId.parse(fromStream.streamName);
        EventStoreIterator iterator = store.fromStream(eventId);
        String iteratorId = remoteIterators.add(fromStream.timeout, fromStream.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    ClusterMessage fromStreams(FromStreams fromStreams) {
        EventStoreIterator iterator = store.fromStreams(fromStreams.eventMap);
        String iteratorId = remoteIterators.add(fromStreams.timeout, fromStreams.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    ClusterMessage onIteratorNext(IteratorNext iteratorNext) {
        List<EventRecord> records = remoteIterators.nextBatch(iteratorNext.uuid);
        return new EventBatch(records);
    }

    @Override
    public void close() {
        remoteIterators.close();
    }
}
