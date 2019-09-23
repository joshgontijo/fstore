package io.joshworks.fstore.core.metrics;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MetricRegistry {

    private static final Map<String, JmxMetricsBean> table = new ConcurrentHashMap<>();

    private static final long UPDATE_INTERVAL = 3000;

    public static String register(String type, Supplier<Metrics> supplier) {
        return register(Map.of("type", type), supplier);
    }

    public static String register(Map<String, String> attributes, Supplier<Metrics> supplier) {
        ObjectName objectName = objectName(attributes);
        String key = objectName.getCanonicalName();
        table.compute(objectName.getCanonicalName(), (k, v) -> v == null ? registerBean(objectName, supplier) : v);
        return key;
    }

    private static ObjectName objectName(Map<String, String> attributes) {
        try {
            String attMap = attributes.entrySet().stream().map(kv -> kv.getKey() + "=" + kv.getValue()).collect(Collectors.joining(","));
            return new ObjectName("fstore:" + attMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static JmxMetricsBean registerBean(ObjectName objectName, Supplier<Metrics> metrics) {
        try {
            var metricsBean = new JmxMetricsBean(objectName, metrics, UPDATE_INTERVAL);
            ManagementFactory.getPlatformMBeanServer().registerMBean(metricsBean, objectName);
            return metricsBean;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void unregister(ObjectName objectName) {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void remove(String key) {
        JmxMetricsBean removed = table.remove(key);
        if (removed != null) {
            unregister(removed.objectName);
        }
    }

    public static void clear() {
        table.values().forEach(jmxBean -> unregister(jmxBean.objectName));
        table.clear();
    }

    private static class JmxMetricsBean implements DynamicMBean {

        private final Supplier<Metrics> supplier;
        private final ObjectName objectName;
        private final long updateInterval;
        private long lastUpdate;
        private Map<String, Long> metrics;

        JmxMetricsBean(ObjectName objectName, Supplier<Metrics> supplier, long updateInterval) {
            this.updateInterval = updateInterval;
            this.supplier = supplier;
            this.objectName = objectName;
        }

        private Map<String, Long> get() {
            long now = System.currentTimeMillis();
            if ((now - lastUpdate) >= updateInterval) {
                lastUpdate = now;
                Metrics metrics = supplier.get();
                this.metrics = metrics.items;
            }
            return metrics;

        }

        @Override
        public Object getAttribute(String attribute) {
            return get().get(attribute);
        }

        @Override
        public void setAttribute(Attribute attribute) {
            //do nothing
        }

        @Override
        public AttributeList getAttributes(String[] attributes) {
            AttributeList list = new AttributeList();
            for (Map.Entry<String, Long> kv : get().entrySet()) {
                list.add(new Attribute(kv.getKey(), kv.getValue()));
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
            Set<String> keys = get().keySet();
            MBeanAttributeInfo[] dAttributes = new MBeanAttributeInfo[keys.size()];

            // Dynamically Build one attribute per thread.
            List<String> items = new ArrayList<>(keys);
            for (int i = 0; i < dAttributes.length; i++) {
                dAttributes[i] = new MBeanAttributeInfo(items.get(i), Long.class.getSimpleName(), "(no description)", true, false, false);
            }

            return new MBeanInfo(this.getClass().getName(), null, dAttributes, null, null, new MBeanNotificationInfo[0]);
        }

    }


}
