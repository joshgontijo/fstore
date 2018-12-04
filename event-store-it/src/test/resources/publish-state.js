config({
    name: "publish-state",
    streams: ["test-stream"],
    type: "ONE_TIME",
    parallel: false,
    batchSize: 10
});

state({
    count: 0,
});

function onEvent(event, state) {
    state.count++;
    if(state.count % 10000 == 0) {
        emit("my-state", {type: "STATE_UPDATED", body: state});
        print("Processed: " + state.count)
    }
}

function aggregateState(state1, state2) {

}