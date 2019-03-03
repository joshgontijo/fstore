config({
    name: "state-inc-value",
    streams: ["test-stream"],
    type: "ONE_TIME",
    parallel: false,
    batchSize: 10,
    publishState: false
});

state({
    evCounter: 0,
    valueSum: 0
});

function onEvent(event, state) {
    state.valueSum += event.data.value;
    state.evCounter++;
    if (state.evCounter % 10000 == 0) {
        print("Processed: " + state.evCounter)
    }
}

function aggregateState(state1, state2) {

}