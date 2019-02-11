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
import io.joshworks.eventry.server.cluster.commands.Append;
import io.joshworks.eventry.server.cluster.commands.AppendError;
import io.joshworks.eventry.server.cluster.commands.AppendSuccess;
import io.joshworks.eventry.server.cluster.commands.IteratorClose;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.LogIterator;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.util.Buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

//CLIENT
public class RemoteStoreClient implements IEventStore {

    private final MessageDispatcher dispatcher;
    private final Address target;

    public RemoteStoreClient(MessageDispatcher dispatcher, Address target) {
        this.dispatcher = dispatcher;
        this.target = target;
    }

    private Message send(byte[] data) {
        try {
            return dispatcher.sendMessage(target, new Buffer(data), RequestOptions.SYNC());
        } catch (Exception e) {
            throw new RemoteClientException(e);
        }
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
        Append append = new Append(event, expectedVersion);
        Message response = send(append.toBytes());
        ByteBuffer bb = ByteBuffer.wrap(response.buffer());
        int code = bb.getInt();
        if(AppendSuccess.CODE == code) {
            return null;
        }
        if(AppendError.CODE == code) {
            AppendError error = new AppendError(bb);
            throw new RuntimeException("REMOTE ERROR CODE: " + error.errorCode);
        }
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
        return null;
    }

    @Override
    public LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
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

        private final MessageDispatcher dispatcher;
        private final Address target;
        private final String uuid;

        private RemoteStoreClientIterator(MessageDispatcher dispatcher, Address target, String uuid) {
            this.dispatcher = dispatcher;
            this.target = target;
            this.uuid = uuid;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public void close() throws IOException {
            byte[] bytes = new IteratorClose(uuid).toBytes();
            dispatcher.sendMessage(target, new Buffer(bytes), RequestOptions.SYNC());
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public EventRecord next() {
            return null;
        }
    }

}
