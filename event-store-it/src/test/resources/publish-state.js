config({
    name: "publish-state",
    streams: ["test-stream"],
    type: "ONE_TIME",
    parallel: false,
    batchSize: 10,
    publishState: true
});

state({
    count: 0,
});

function onEvent(event, state) {
    state.count++;
    if(state.count % 10000 == 0) {
        print("Processed: " + state.count)
    }
}

function aggregateState(state1, state2) {

}