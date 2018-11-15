var out_events = [];

function process_events(events, state) {
    out_events = [];
    while(!events.isEmpty()) {
        var event = events.peek();
        onEvent(event, state);
        events.poll()
    }
    return out_events;
}

//String dstStream, String sourceStream, int sourceVersion, String sourceType
function linkTo(stream, event, metadata) {
    out_events.push({
        type: "LINK_TO",
        dstStream: stream,
        srcStream: event.stream,
        srcVersion: event.version,
        srcType: event.type,
        metadata: metadata
    })
}

function emit(stream, event) {
    if (!event) {
        throw Error("Event must be provided")
    }
    if (!event.type || (this.length === 0 || !this.trim())) {
        throw Error("Event type must be provided")
    }
    if (event.data === null || typeof event.data !== 'object') {
        throw Error("Event data must be provided")
    }

    if (event.metadata !== null && typeof event.metadata !== 'object') {
        throw Error("Event metadata must be an object")
    }

    out_events.push({type: "EMIT", stream: stream, event: event})
}


