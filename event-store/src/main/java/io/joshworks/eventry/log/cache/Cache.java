package io.joshworks.eventry.log.cache;

import java.util.function.Predicate;

public interface Cache {

    CachedEntry get(long position);

    void put(long position, CachedEntry cachedEntry);

    void removeIf(Predicate<CachedEntry> entry);

    long size();

    int entries();

    //TODO add cache hit and cache miss ??

}
