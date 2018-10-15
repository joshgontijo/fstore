package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.Block;

public interface IBlockAppender<T, B extends Block<T>, L extends Log<B>> extends Appender<B, L> {

    LogIterator<T> entryIterator();

}
