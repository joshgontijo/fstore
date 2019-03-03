package io.joshworks.eventry.projection.result;

import io.joshworks.eventry.IEventAppender;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.projection.JsonEvent;
import io.joshworks.eventry.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ScriptExecutionResult {

    private static final String EMIT = "EMIT";
    private static final String LINK_TO = "LINK_TO";

    public final List<OutputEvent> outputEvents = new ArrayList<>();
    public final int emittedEvents;
    public final int linkToEvents;

    public ScriptExecutionResult(List<Map<String, Object>> outputQueue) {
        int emitted = 0;
        int linked = 0;
        for (Map<String, Object> item : outputQueue) {
            String type = String.valueOf(item.get("type"));
            if (EMIT.equals(type)) {
                String stream = String.valueOf(item.get("stream"));
                Map<String, Object> event = (Map<String, Object>) item.get("event");
                outputEvents.add(new EmitEvent(stream, event));
                emitted++;

            } else if (LINK_TO.equals(type)) {
                String dstStream = String.valueOf(item.get("dstStream"));
                String srcStream = String.valueOf(item.get("srcStream"));
                String srcType = String.valueOf(item.get("srcType"));
                int srcVersion = (int) item.get("srcVersion");
                outputEvents.add(new LinkToEvent(dstStream, srcStream, srcVersion, srcType));
                linked++;
            }
        }
        this.emittedEvents = emitted;
        this.linkToEvents = linked;
    }

    public interface OutputEvent {
        void handle(IEventAppender storeAppender);
    }

    private static class LinkToEvent implements OutputEvent {

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
        public void handle(IEventAppender appender) {
            appender.linkTo(dstStream, StreamName.of(srcStream, srcVersion), srcType);
        }
    }

    private static class EmitEvent implements OutputEvent {

        private final Map<String, Object> event;

        private EmitEvent(String dstStream, Map<String, Object> event) {
            this.event = event;
            this.event.put("stream", dstStream);
        }

        @Override
        public void handle(IEventAppender appender) {
            JsonEvent jsonEVent = JsonEvent.fromMap(event);
            appender.append(jsonEVent.toEvent());
        }
    }

}
