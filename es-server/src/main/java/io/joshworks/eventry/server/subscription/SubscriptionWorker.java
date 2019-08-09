package io.joshworks.eventry.server.subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionWorker implements Runnable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionWorker.class);

    private static final Map<String, ClientSubscription> items = new ConcurrentHashMap<>();
    private final int waitTime;

    private final AtomicBoolean closed = new AtomicBoolean();

    public SubscriptionWorker(int waitTime) {
        this.waitTime = waitTime;
    }

    boolean add(ClientSubscription clientSubscription) {
        if (items.containsKey(clientSubscription.subscriptionId())) {
            logger.warn("Subscription with id {} already exist", clientSubscription.subscriptionId());
            return false;
        }
        items.put(clientSubscription.subscriptionId(), clientSubscription);
        return true;
    }

    void remove(String subscriptionId) {
        ClientSubscription clientSubscription = items.remove(subscriptionId);
        if (clientSubscription == null) {
            throw new IllegalArgumentException("No subscription for id: " + subscriptionId);
        }
        clientSubscription.close();
    }

    void disable(String subscriptionId) {
        ClientSubscription clientSubscription = items.get(subscriptionId);
        if (clientSubscription == null) {
            throw new IllegalArgumentException("No subscription for id: " + subscriptionId);
        }
        clientSubscription.disable();
    }

    void enable(String subscriptionId) {
        ClientSubscription clientSubscription = items.get(subscriptionId);
        if (clientSubscription == null) {
            throw new IllegalArgumentException("No subscription for id: " + subscriptionId);
        }
        clientSubscription.enable();
    }


    @Override
    public void run() {
        try {
            while (!closed.get()) {
                int totalSent = 0;
                for (Map.Entry<String, ClientSubscription> kv : items.entrySet()) {
                    ClientSubscription subscription = kv.getValue();
                    totalSent += subscription.send();
                    if (!subscription.isOpen()) {
                        subscription.close();
                        items.remove(kv.getKey()); //TODO check thread safety here without using iterator
                    }
                }
                if (totalSent == 0) {
                    Thread.sleep(waitTime);
                }
            }

        } catch (InterruptedException e) {
            logger.warn("Thread interrupted while waiting");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Unhandled error while running subscription worker, worker will be terminated", e);
            close();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        Iterator<Map.Entry<String, ClientSubscription>> iterator = items.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ClientSubscription> clientSubscription = iterator.next();
            logger.info("Closing active subscription: {}", clientSubscription.getKey());
            clientSubscription.getValue().close();
            iterator.remove();
        }
    }
}
