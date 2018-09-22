package io.joshworks.eventry.projections;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.log.EventRecord;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ScriptAPI {

    final EventStore store;
    final Function<String, SingleStream> fromStream;
    final Function<String[], SingleStream> fromStreams;
    final Function<String[], ForEachStream> foreachstream;
    final BiConsumer<String, JsonEvent> linkTo;
    final BiConsumer<String, JsonEvent> emit;

    ScriptAPI(EventStore store, Consumer<ExecutionStatus> executionStatusListener, Supplier<Boolean> shutdownRequest) {
        this.store = store;
        this.fromStream = s -> new SingleStream(store.fromStream(s).map(JsonEvent::from), executionStatusListener, shutdownRequest);
        this.fromStreams = streams -> new SingleStream(store.zipStreams(Set.of(streams)).map(JsonEvent::from), executionStatusListener, shutdownRequest);
        this.foreachstream = streams -> {
            Set<String> streams1 = Set.of(streams);
            Map<String, Stream<EventRecord>> mapped = store.fromStreamsMapped(streams1);
            Map<String, Stream<JsonEvent>> mappedStream = mapped.entrySet().stream()
                    .map(kv -> new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue().map(JsonEvent::from)))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

            return new ForEachStream(mappedStream, executionStatusListener, shutdownRequest);

        };

        this.linkTo = (stream, event) -> store.linkTo(stream, event.toEvent());
        this.emit = (stream, event) -> store.emit(stream, event.toEvent());
    }


}
