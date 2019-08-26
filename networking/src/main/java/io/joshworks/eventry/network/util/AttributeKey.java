package io.joshworks.eventry.network.util;

public class AttributeKey<T>  {
    private final Class<T> valueType;

    private AttributeKey(Class<T> valueType) {
        this.valueType = valueType;
    }

    public T cast(final Object value) {
        return valueType.cast(value);
    }

    public static <T> AttributeKey<T> create(Class<? super T> valueClass) {
        return new AttributeKey(valueClass);
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