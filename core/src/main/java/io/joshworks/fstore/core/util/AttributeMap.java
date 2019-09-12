package io.joshworks.fstore.core.util;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class AttributeMap {

    private Map<AttributeKey<?>, Object> attachments = new ConcurrentHashMap<>();

    public <T> T getAttachment(final AttributeKey<T> key) {
        if (key == null || attachments == null) {
            return null;
        }
        return (T) attachments.get(key);
    }

    public <T> T putAttachment(final AttributeKey<T> key, final T value) {
        Objects.requireNonNull(key, "Must be provided");
        return (T) attachments.put(key, value);
    }

    public <T> T removeAttachment(final AttributeKey<T> key) {
        if (key == null || attachments == null) {
            return null;
        }
        return (T) attachments.remove(key);
    }

}
