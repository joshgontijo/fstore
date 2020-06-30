package io.joshworks.ilog;

import java.io.File;

@FunctionalInterface
public interface SegmentFactory<T extends IndexedSegment> {

    T create(File file, int indexSize);

}
