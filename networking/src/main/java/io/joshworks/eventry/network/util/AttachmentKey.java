package io.joshworks.eventry.network.util;

public class AttachmentKey<T>  {
    private final Class<T> valueType;

    private AttachmentKey(Class<T> valueType) {
        this.valueType = valueType;
    }

    public T cast(final Object value) {
        return valueType.cast(value);
    }

    public static <T> AttachmentKey<T> create(Class<? super T> valueClass) {
        return new AttachmentKey(valueClass);
    }

    @Override
    public String toString() {
        if (valueType != null) {
            StringBuilder sb = new StringBuilder(getClass().getName());
            sb.append("<");
            sb.append(valueType.getName());
            sb.append(">");
            return sb.toString();
        }
        return super.toString();
    }
}