package io.joshworks.fstore.es.stream;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.es.LRUCache;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Streams {

    //TODO LRU cache ? there's no way of getting item by stream name, need to use an indexed lsm-tree
    //LRU map that reads the last version from the index
    private final LRUCache<Long, AtomicInteger> versions;
    private final Map<Long, EventStream> streamsMap;
    private final SimpleLogAppender<EventStream> appender;


    public Streams(File root, int versionLruCacheSize, Function<Long, Integer> versionFetcher) {
        this.appender = new SimpleLogAppender<>(LogAppender.builder(new File(root, "streams"), new EventStreamSerializer()));
        this.streamsMap = loadFromDisk(this.appender);
        this.versions = new LRUCache<>(versionLruCacheSize, streamHash -> new AtomicInteger(versionFetcher.apply(streamHash)));
    }

    public Optional<EventStream> get(long streamHash) {
        return Optional.ofNullable(streamsMap.get(streamHash));
    }

    //TODO implement 'remove'
    //TODO field validation needed
    public void add(EventStream stream) {
        Objects.requireNonNull(stream);
        appender.append(stream);
        streamsMap.put(stream.hash, stream);
    }

    public Set<String> streamsStartingWith(String value) {
        return streamsMap.values().stream()
                .filter(stream -> stream.name.startsWith(value))
                .map(stream -> stream.name)
                .collect(Collectors.toSet());
    }

    public int version(long stream) {
        return versions.getOrElse(stream, new AtomicInteger(-1)).get();
    }

    public int tryIncrementVersion(long stream, int expected) {
        AtomicInteger versionCounter = versions.getOrElse(stream, new AtomicInteger(-1));
        if(expected < 0) {
            return versionCounter.incrementAndGet();
        }
        int newValue = versionCounter.compareAndExchange(expected, expected);
        if(newValue != expected) {
            throw new IllegalArgumentException("Version mismatch: expected stream " + stream + " version: " + expected + ", got " + newValue);
        }
        return newValue;
    }


    private static Map<Long, EventStream> loadFromDisk(SimpleLogAppender<EventStream> appender) {
        Map<Long, EventStream> map = new ConcurrentHashMap<>();
        try (LogIterator<EventStream> scanner = appender.scanner()) {

            while (scanner.hasNext()) {
                EventStream next = scanner.next();
                map.put(next.hash, next);
            }

        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }

        return map;

    }

}
