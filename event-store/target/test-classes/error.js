config({
    name: "by-type",
    streams: ["_all"],
    type: "CONTINUOUS",
    parallel: false
});

function onEvent(event, state) {
    someInvalidMethodName(event)
}

function aggregateState(state1, state2) {

}