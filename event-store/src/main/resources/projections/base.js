var _out_events = [];

function _process_events(events, state) {
    _out_events = [];
    while(!events.isEmpty()) {
        var event = events.peek();
        onEvent(event, state);
        events.poll();
    }
    return _out_events;
}

//aggregateState is an optional function, that should be defined only for parallel computation
function _aggregate_state(state1, state2) {
    if (typeof aggregateState === "function") {
        return aggregateState(state1, state2);
    }
    return {};
}

//onStart is an optional function
function _onStart(state) {
    if (typeof onStart === "function") {
        onStart(state);
        return _out_events;
    }
    return {};
}

//onStart is an optional function
function _onStop(reason, state) {
    if (typeof onStop === "function") {
        onStop(reason, state);
        return _out_events;
    }
    return {};
}

function linkTo(stream, event, metadata) {
    _out_events.push({
        type: "LINK_TO",
        dstStream: stream,
        srcStream: event.stream,
        srcVersion: event.version,
        srcType: event.type,
        metadata: metadata
    });
}

function emit(stream, event) {
    if (!event) {
        throw Error("Event must be provided");
    }
    if (!event.type || (event.type.length === 0 || !event.type.trim())) {
        throw Error("Event type must be provided");
    }
    if (event.body === null || typeof event.body !== 'object') {
        throw Error("Event data must be provided");
    }

    if ((event.metadata && event.metadata !== null) &&  typeof event.metadata !== 'object') {
        throw Error("Event metadata must be an object");
    }

    _out_events.push({type: "EMIT", stream: stream, event: event});
}


