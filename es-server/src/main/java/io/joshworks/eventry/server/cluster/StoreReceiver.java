package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.StreamName;
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
import io.joshworks.fstore.log.LogIterator;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StoreReceiver implements Closeable {

    private final RemoteIterators remoteIterators = new RemoteIterators();
    private final IEventStore store;

    public StoreReceiver(IEventStore store) {
        this.store = store;
    }

    AppendResult append(Append append) {
        store.append(append.event, append.expectedVersion);
        return new AppendResult(true);
    }

    ClusterMessage get(Get get) {
        StreamName streamName = StreamName.parse(get.streamName);
        EventRecord eventRecord = store.get(streamName);
        return new EventData(eventRecord);
    }

    ClusterMessage fromAll(FromAll fromAll) {
        LogIterator<EventRecord> iterator = store.fromAll(fromAll.linkToPolicy, fromAll.systemEventPolicy);
        String iteratorId = remoteIterators.add(fromAll.timeout, fromAll.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    ClusterMessage fromStream(FromStream fromStream) {
        StreamName streamName = StreamName.parse(fromStream.streamName);
        LogIterator<EventRecord> iterator = store.fromStream(streamName);
        String iteratorId = remoteIterators.add(fromStream.timeout, fromStream.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    ClusterMessage fromStreams(FromStreams fromStreams) {
        Set<StreamName> streams = fromStreams.streams.stream().map(StreamName::parse).collect(Collectors.toSet());
        LogIterator<EventRecord> iterator = store.fromStreams(streams, fromStreams.ordered);
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
