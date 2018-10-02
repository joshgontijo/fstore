config({
    name: "by-type",
    source: ["clickstream"],
    type: "ONE_TIME",
    parallel: false,
    enabled: true
});

function filter(event, state) {
    return true;
}

function onEvent(event, state) {
    const evType = event.data.eventType;
    if(!state[evType])
        state[evType] = 0;
    state[evType] += state[evType] + 1
}

function aggregateState(state1, state2) {

}