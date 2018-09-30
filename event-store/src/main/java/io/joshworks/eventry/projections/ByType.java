package io.joshworks.eventry.projections;

public class ByType extends JavaHandler {

    public ByType(ProjectionContext context) {
        super(context);
    }

    @Override
    public boolean filter(JsonEvent record, State state) {
        return true;
    }

    @Override
    public void onEvent(JsonEvent record, State state) {
        context.linkTo(record.type, record);
    }

}
