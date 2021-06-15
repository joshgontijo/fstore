config({
    name: "by-type",
    streams: ["_all"],
    type: "CONTINUOUS",
    parallel: false,
    batchSize: 10000,
    publishState: false
});

function onEvent(event, state) {
    linkTo(event.type, event)
}

function aggregateState(state1, state2) {

}