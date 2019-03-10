config({
    name: "by-value",
    streams: ["test-stream"],
    type: "ONE_TIME",
    parallel: false,
    batchSize: 10,
    publishState: false
});

function onStart(state) {

}

function onEvent(event, state) {
    linkTo(event.data.value, event)
}

function onStop(reason, state) {

}

function aggregateState(state1, state2) {

}