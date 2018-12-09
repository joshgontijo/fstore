config({
    name: "on-stop",
    streams: ["test-stream"],
    type: "ONE_TIME",
    parallel: false,
    batchSize: 10
});

state({
    started: false,
    stopped: false
});

function onStart(state) {
    state.started = true;
    emit("my-state", {type: "STARTED", body: state});
}

function onStop(reason, state) {
    state.stopped = true;
    emit("my-state", {type: "STOPPED", body: state});
}

function onEvent(event, state) {
    //do nothing
}

function aggregateState(state1, state2) {

}