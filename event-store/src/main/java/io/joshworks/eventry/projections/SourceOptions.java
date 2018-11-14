//package io.joshworks.eventry.projections;
//
//import io.joshworks.eventry.data.Constant;
//
//import java.util.LinkedList;
//import java.util.Set;
//
//public class SourceOptions {
//
//    public final Set<String> streams;
//    public final boolean parallel;
//
//    public SourceOptions(Projection projection) {
//        this.streams = projection.sources;
//        this.parallel = projection.parallel;
//    }
//
//    public boolean isSingleSource() {
//        //zipped streams is single source
//        return streams.size() == 1 || !parallel;
//    }
//
//    public boolean isAllStream() {
//        return streams.size() == 1 && Constant.ALL_STREAMS.equals(new LinkedList<>(streams).getFirst());
//    }
//
//}
