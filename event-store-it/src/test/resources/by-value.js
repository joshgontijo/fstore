config({
    name: "by-value",
    streams: ["test-stream"],
    type: "ONE_TIME",
    parallel: false,
    batchSize: 10,
    publishState: false
});

function onEvent(event, state) {
    linkTo(event.data.value, event)
}

function aggregateState(state1, state2) {

}