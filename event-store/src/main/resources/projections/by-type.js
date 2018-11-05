config({
    name: "by-type",
    streams: ["_all"],
    type: "CONTINUOUS",
    parallel: false,
    enabled: true
});

function filter(event, state) {
    return true;
}

function onEvent(event, state) {
    linkTo(event.type, event)
}

function aggregateState(state1, state2) {

}