package io.joshworks.fstore.projection.scratch;

public class Scratch {

    //private void loadProjections() {
    //        logger.info("Loading projections");
    //        long start = System.currentTimeMillis();
    //
    //        Set<String> running = new HashSet<>();
    //
    //        fromStream(StreamName.of(SystemStreams.PROJECTIONS))
    //                .when(ProjectionCreated.TYPE, ev -> projections.add(ProjectionCreated.from(ev)))
    //                .when(ProjectionUpdated.TYPE, ev -> projections.add(ProjectionCreated.from(ev)))
    //                .when(ProjectionDeleted.TYPE, ev -> projections.delete(ProjectionDeleted.from(ev).name))
    //                .when(ProjectionStarted.TYPE, ev -> running.add(ProjectionStarted.from(ev).name))
    //                .when(ProjectionResumed.TYPE, ev -> running.add(ProjectionResumed.from(ev).name))
    //                .when(ProjectionStopped.TYPE, ev -> running.remove(ProjectionStopped.from(ev).name))
    //                .when(ProjectionFailed.TYPE, ev -> running.remove(ProjectionFailed.from(ev).name))
    //                .match();
    //
    //
    //        int loaded = projections.all().size();
    //        logger.info("Loaded {} projections in {}ms", loaded, (System.currentTimeMillis() - start));
    //        if (loaded == 0) {
    //            logger.info("Creating system projections");
    //            for (Projection projection : this.projections.loadSystemProjections()) {
    //                createProjection(projection);
    //                projections.add(projection);
    //            }
    //        }
    //
    //        projections.bootstrapProjections(this, running);
    //    }

}
