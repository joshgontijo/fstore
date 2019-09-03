package io.joshworks.fstore.core.metrics;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.ObjectName;
import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Metrics implements DynamicMBean, Closeable {

    private final Map<String, Metric> table = new ConcurrentHashMap<>();

    Metrics(String type) {
        try {
            final ObjectName objectName = new ObjectName("fstore:type=" + type);
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void update(String name) {
        update(name, 1);
    }

    public void update(String name, long delta) {
        table.compute(name, (k, v) -> {
            if (v == null) {
                v = new StandardMetric();
            }
            v.update(delta);
            return v;
        });
    }

    public void set(String name, long value) {
        table.compute(name, (k, v) -> {
            if (v == null) {
                v = new StandardMetric();
            }
            v.set(value);
            return v;
        });
    }

    public void register(String name, Supplier<Long> supplier) {
        table.put(name, new SupplierMetric(supplier));
    }

    public void register(String name, Consumer<AtomicLong> supplier) {
        table.put(name, new ConsumerMetric(supplier));
    }

    @Override
    public Object getAttribute(String attribute) {
        Metric val = table.get(attribute);
        return val == null ? null : val.get();
    }

    @Override
    public void setAttribute(Attribute attribute) {
        //do nothing
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();
        for (Map.Entry<String, Metric> kv : table.entrySet()) {
            list.add(new Attribute(kv.getKey(), kv.getValue().get()));
        }
        return list;
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        return null;
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) {
        return null;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        MBeanAttributeInfo[] dAttributes = new MBeanAttributeInfo[table.keySet().size()];

        // Dynamically Build one attribute per thread.
        List<String> items = new ArrayList<>(table.keySet());
        for (int i = 0; i < dAttributes.length; i++) {
            dAttributes[i] = new MBeanAttributeInfo(items.get(i), Long.class.getSimpleName(), "(no description)", true, false, false);
        }

        return new MBeanInfo(this.getClass().getName(), null, dAttributes, null, null, new MBeanNotificationInfo[0]);
    }

    @Override
    public void close() {
        table.clear();
    }
}
