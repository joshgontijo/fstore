package io.joshworks.eventry.projections.result;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.JsonEvent;
import io.joshworks.eventry.utils.StringUtils;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class ScriptExecutionResult {

    private static final String EMIT = "EMIT";
    private static final String LINK_TO = "LINK_TO";

    public final Queue<OutputEvent> outputEvents = new ArrayDeque<>();
    public final int emittedEvents;
    public final int linkToEvents;

    public ScriptExecutionResult(List<Map<String, Object>> outputQueue) {
        int emittedEvents = 0;
        int linkToEvents = 0;
        for (Map<String, Object> item : outputQueue) {
            String type = String.valueOf(item.get("type"));
            if (EMIT.equals(type)) {
                String stream = String.valueOf(item.get("stream"));
                Map<String, Object> event = (Map<String, Object>) item.get("event");
                outputEvents.offer(new EmitEvent(stream, event));
                emittedEvents++;

            } else if (LINK_TO.equals(type)) {
                String dstStream = String.valueOf(item.get("dstStream"));
                String srcStream = String.valueOf(item.get("srcStream"));
                String srcType = String.valueOf(item.get("srcType"));
                int srcVersion = (int) item.get("srcVersion");
                outputEvents.offer(new LinkToEvent(dstStream, srcStream, srcVersion, srcType));
                linkToEvents++;
            }
        }
        this.emittedEvents = emittedEvents;
        this.linkToEvents = linkToEvents;
    }

    public abstract static class OutputEvent {
        public abstract void handle(IEventStore store);
    }

    private static class LinkToEvent extends OutputEvent {

        private final String dstStream;
        private final String srcStream;
        private final int srcVersion;
        private final String srcType;

        private LinkToEvent(String dstStream, String srcStream, int srcVersion, String srcType) {
            this.dstStream = StringUtils.requireNonBlank(dstStream);
            this.srcStream = StringUtils.requireNonBlank(srcStream);
            this.srcVersion = srcVersion;
            this.srcType = StringUtils.requireNonBlank(srcType);
        }


        @Override
        public void handle(IEventStore store) {
            store.linkTo(dstStream, srcStream, srcVersion, srcType);
        }
    }

    private static class EmitEvent extends OutputEvent {

        private final Map<String, Object> event;

        private EmitEvent(String dstStream, Map<String, Object> event) {
            this.event = event;
            this.event.put("stream", dstStream);
        }

        @Override
        public void handle(IEventStore store) {
            JsonEvent jsonEVent = JsonEvent.fromMap(event);
            store.append(jsonEVent.toEvent());
        }
    }

}
