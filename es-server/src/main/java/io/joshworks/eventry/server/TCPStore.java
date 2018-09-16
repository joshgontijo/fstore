package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.TcpClient;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class TCPStore implements EventStore {

    //TODO this should be a pool of many clients
    //only one connection is support at this moment
    private final TcpClient client;

    public TCPStore(TcpClient client) {
        this.client = client;
    }


    @Override
    public LogIterator<IndexEntry> keys() {
        return null;
    }

    @Override
    public void cleanup() {

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
    public LogIterator<EventRecord> fromStreamIter(String stream) {
        return null;
    }

    @Override
    public Stream<EventRecord> fromStream(String stream) {
        return null;
    }

    @Override
    public LogIterator<EventRecord> fromStreamIter(String stream, int versionInclusive) {
        return null;
    }

    @Override
    public Stream<EventRecord> fromStream(String stream, int versionInclusive) {
        return null;
    }

    @Override
    public Stream<EventRecord> zipStreams(Set<String> streams) {
        return null;
    }

    @Override
    public LogIterator<EventRecord> zipStreamsIter(String stream) {
        return null;
    }

    @Override
    public Stream<EventRecord> zipStreams(String streamPrefix) {
        return null;
    }

    @Override
    public LogIterator<EventRecord> zipStreamsIter(Set<String> streamNames) {
        return null;
    }

    @Override
    public Stream<Stream<EventRecord>> fromStreams(Set<String> streams) {
        return null;
    }

    @Override
    public Map<String, Stream<EventRecord>> fromStreamsMapped(Set<String> streams) {
        return null;
    }

    @Override
    public int version(String stream) {
        return 0;
    }

    @Override
    public LogIterator<EventRecord> fromAllIter() {
        return null;
    }

    @Override
    public Stream<EventRecord> fromAll() {
        return null;
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        return null;
    }

    @Override
    public void emit(String stream, EventRecord event) {

    }

    @Override
    public EventRecord get(String stream, int version) {
        return null;
    }

    @Override
    public EventRecord get(IndexEntry indexEntry) {
        return null;
    }

    @Override
    public EventRecord append(EventRecord event) {
        return null;
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        return null;
    }

    @Override
    public PollingSubscriber<EventRecord> poller() {
        return null;
    }

    @Override
    public PollingSubscriber<EventRecord> poller(long position) {
        return null;
    }

    @Override
    public PollingSubscriber<EventRecord> poller(String stream) {
        return null;
    }

    @Override
    public PollingSubscriber<EventRecord> poller(Set<String> streamNames) {
        return null;
    }

    @Override
    public long logPosition() {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void add(EventRecord record) {

    }



}
