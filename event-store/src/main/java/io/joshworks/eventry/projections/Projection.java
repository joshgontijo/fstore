package io.joshworks.eventry.projections;

import java.util.Set;

public class Projection {

    public final String script;
    public final String name;
    public final String engine;
    public final Set<String> sources;
    public final Type type;
    public final boolean parallel;

    public boolean enabled;

    public Projection(String script, String name, String engine, Set<String> sources, Type type, boolean parallel) {
        this.script = script;
        this.name = name;
        this.engine = engine;
        this.sources = sources;
        this.type = type;
        this.parallel = parallel;
    }

    public enum Type {
        CONTINUOUS,
        ONE_TIME,
        AD_HOC

    }

}
