package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;

import java.io.File;

public class BlockConfig<T> extends Config<T> {
    BlockConfig(File directory, Serializer<T> serializer) {
        super(directory, serializer);
    }


}
