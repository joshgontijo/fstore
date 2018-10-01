options({

});

source({
    streams: ["stream1"],
    parallel: false
});


function filter(event, state) {
    return true;
}

function onEvent(event, state) {

}

function aggregateState(state1, state2) {

}