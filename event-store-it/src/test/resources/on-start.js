config({
    name: "on-start",
    streams: ["test-stream"],
    type: "ONE_TIME",
    parallel: false,
    batchSize: 10
});

state({
    started: false,
});

function onStart() {
    state.started = true
}

function onStop(reason, state) {
    emit("my-state", {type: "STATE_UPDATED", body: state});
}

function onEvent(event, state) {
}

function aggregateState(state1, state2) {

}