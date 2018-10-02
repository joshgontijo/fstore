options({

});

source({
    streams: ["github"],
    parallel: false
});


function filter(event, state) {
    return true;
}

function onEvent(event, state) {
    linkTo(event.type, event)
}

function aggregateState(state1, state2) {

}