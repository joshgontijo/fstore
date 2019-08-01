package io.joshworks.eventry.network.util;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class AttachmentMap {

    private Map<AttachmentKey<?>, Object> attachments = new ConcurrentHashMap<>();

    public <T> T getAttachment(final AttachmentKey<T> key) {
        if (key == null || attachments == null) {
            return null;
        }
        return (T) attachments.get(key);
    }

    public <T> T putAttachment(final AttachmentKey<T> key, final T value) {
        Objects.requireNonNull(key, "Must be provided");
        return (T) attachments.put(key, value);
    }

    public <T> T removeAttachment(final AttachmentKey<T> key) {
        if (key == null || attachments == null) {
            return null;
        }
        return (T) attachments.remove(key);
    }

}
