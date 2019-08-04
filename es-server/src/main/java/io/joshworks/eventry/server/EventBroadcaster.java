package io.joshworks.eventry.server;

import com.google.gson.Gson;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.shared.JsonEvent;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import io.joshworks.snappy.sse.EventData;
import io.joshworks.snappy.sse.SseBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EventBroadcaster implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(EventBroadcaster.class);

    private static final String THREAD_NAME_PREFIX = "broadcast-worker-";
    private final AtomicBoolean closed = new AtomicBoolean();

    private ExecutorService executor;
    private final int numWorkers;

    private final AtomicInteger threadCount = new AtomicInteger();
    private Set<BroadcastWorker> workers = new HashSet<>();

    public EventBroadcaster(long waitTime, int numWorkers) {
        this.numWorkers = numWorkers;
        if (numWorkers == 0) {
            return;
        }
        this.executor = Executors.newFixedThreadPool(numWorkers, r -> {
            Thread thread = new Thread(r);
            thread.setName(THREAD_NAME_PREFIX + threadCount.getAndIncrement());
            return thread;
        });

        logger.info("Starting {} broadcast workers", numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            BroadcastWorker worker = new BroadcastWorker(waitTime);
            workers.add(worker);
            this.executor.submit(worker);
        }

    }


    public boolean add(LogIterator<EventRecord> poller) {
        if (closed.get()) {
            logger.warn("Event broadcaster is closed");
            return false;
        }
        if (numWorkers == 0) {
            logger.warn("Event broadcaster has no workers, push is disabled");
            return false;
        }
        if (workers.isEmpty()) {
            logger.warn("No worker is available");
            return false;
        }

        workers.stream().min(Comparator.comparingInt(BroadcastWorker::pollerCount))
                .orElseThrow().pollers.add(poller);

        return true;
    }

    public void remove(LogIterator<EventRecord> poller) {
        for (BroadcastWorker worker : workers) {
            worker.remove(poller);
        }
        IOUtils.closeQuietly(poller);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        logger.info("Shutting down event broadcaster");
        for (BroadcastWorker worker : workers) {
            worker.close();
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }

    private static final class BroadcastWorker implements Runnable, Closeable {

        private static final Logger logger = LoggerFactory.getLogger(EventBroadcaster.class);
        private final Gson gson = new Gson();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final List<LogIterator<EventRecord>> pollers = new ArrayList<>();
        private final long waitTime;

        private BroadcastWorker(long waitTime) {
            this.waitTime = waitTime;
        }

        @Override
        public void run() {
            while (!closed.get()) {
                try {

                    if (pollers.isEmpty()) {
                        Thread.sleep(10000);
                    }

                    List<EventRecord> available = new ArrayList<>();
                    for (LogIterator<EventRecord> poller : pollers) {
                        EventRecord event = poller.next();
                        if (event == null) {
                            continue;
                        }
                        available.add(event);
                    }

                    for (EventRecord event : available) {
                        send(event);
                    }

                    if (available.isEmpty()) {
                        Thread.sleep(waitTime);
                    }

                } catch (Exception e) {
                    logger.error("Error polling event", e);
                }
            }

        }

        @Override
        public void close() {
            closed.set(true);
            for (LogIterator<EventRecord> poller : pollers) {
                remove(poller);
            }
        }

        //TODO expose info about pollers ? position would be interesting but is not implemented atm
        public int pollerCount() {
            return pollers.size();
        }

        private void send(EventRecord event) {
            try {
                Map<String, Object> data = JsonSerializer.toMap(new String(event.body, StandardCharsets.UTF_8));
                Map<String, Object> metadata = JsonSerializer.toMap(new String(event.metadata, StandardCharsets.UTF_8));
                JsonEvent jsonEvent = new JsonEvent(event.type, event.timestamp, event.stream, event.version, data, metadata);

                String eventId = String.valueOf(event.eventId());
                String stream = event.stream;

                EventData eventData = new EventData(JsonSerializer.toJson(jsonEvent), eventId, stream);
                SseBroadcaster.broadcast(eventData, stream);
            } catch (Exception e) {
                logger.error("Error sending event", e);
            }

        }

        public void remove(LogIterator<EventRecord> poller) {
            pollers.remove(poller);
            IOUtils.closeQuietly(poller);
        }

    }

}
