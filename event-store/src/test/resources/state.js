config({
    name: "by-type",
    streams: ["_all"],
    type: "CONTINUOUS",
    parallel: false,
    batchSize: 10000,
    publishState: false
});

state({ count: 10});

function onEvent(event, state) {
    state.count++;
}

function aggregateState(state1, state2) {

}