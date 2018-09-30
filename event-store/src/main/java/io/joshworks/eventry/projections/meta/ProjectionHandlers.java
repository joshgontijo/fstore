package io.joshworks.eventry.projections.meta;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ProjectionHandlers {

   private final Map<String, EventStreamHandler> handlers = new HashMap<>();

    public void register(String name, EventStreamHandler handler) {
       handlers.put(name, handler);
   }

   public void remove(Stream name) {
       handlers.remove(name);
   }

   public EventStreamHandler get(String name) {
       return handlers.get(name);
   }


}
