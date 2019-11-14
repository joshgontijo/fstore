package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.lsmtree.EntryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class TransactionLog<K extends Comparable<K>, V> {

    private final LogAppender<LogRecord> appender;
    final AtomicReference<FlushTask> lastCompletedFlush = new AtomicReference<>(new FlushTask(null, 0, 0));
    private final Map<String, FlushTask> flushItems = new ConcurrentHashMap<>();
    private final Logger logger;

    public TransactionLog(File root, Serializer<K> keySerializer, Serializer<V> valueSerializer, long size, int compactionThreshold, String name, StorageMode mode) {
        String logName = name + "-log";
        this.appender = LogAppender.builder(new File(root, "log"), new LogRecordSerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new LastFlushDiscardCombiner(lastCompletedFlush))
                .name(logName)
                .segmentSize(size)
                .compactionThreshold(compactionThreshold)
                .storageMode(mode)
                .open();

        this.logger = LoggerFactory.getLogger(logName);
    }

    public synchronized long append(LogRecord record) {
        return appender.append(record);
    }

    public synchronized String markFlushing() {
        IndexFlushedStarted record = LogRecord.memFlushStarted(System.currentTimeMillis());
        long position = appender.append(record);
        flushItems.put(record.token, new FlushTask(record.token, record.timestamp, position));
        return record.token;
    }

    public synchronized void markFlushed(String token) {
        appender.append(LogRecord.memFlushed(token));
        FlushTask task = flushItems.remove(token);
        if (task == null) {
            throw new IllegalStateException("Could not find flush task for token " + token);
        }

        //compare the timestamp of the flush tasks and always keep the last one
        lastCompletedFlush.accumulateAndGet(task, (current, newVal) -> {
            int compare = Long.compare(newVal.timestamp, current.timestamp);
            return compare >= 0 ? newVal : current;
        });
    }

    /**
     * The last completed flush position in the log
     *
     * @return The last completed flush
     */
    public long lastFlushedPosition() {
        return lastCompletedFlush.get().position;
    }

    public void restore(Consumer<LogRecord> consumer) {
        logger.info("Restoring log");
        Deque<LogRecord> stack = new ArrayDeque<>();
        Set<String> flushTokens = new HashSet<>();
        try (LogIterator<LogRecord> iterator = appender.iterator(Direction.BACKWARD)) {
            while (iterator.hasNext()) {
                LogRecord record = iterator.next();

                if (EntryType.MEM_FLUSHED.equals(record.type)) {
                    //ignore
                    flushTokens.add(((IndexFlushed) record).token);
                    continue;
                }

                if (EntryType.MEM_FLUSH_STARTED.equals(record.type)) {
                    if (flushTokens.isEmpty()) {
                        //TODO add functionality to logstore to set the write position in the log
                        //then from here, always make sure that the log start at LOG_START, to avoid double entries in the SSTable
                        logger.warn("Found incomplete flush");
                        continue;
                    }
                    IndexFlushedStarted flushStart = (IndexFlushedStarted) record;
                    String token = flushStart.token;
                    if (flushTokens.contains(token)) {
                        lastCompletedFlush.set(new FlushTask(token, record.timestamp, flushStart.position));
                        break;
                    }
                    continue;
                }

                stack.push(record);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (LogRecord kvRecord : stack) {
            consumer.accept(kvRecord);
        }
    }

    public void close() {
        appender.close();
    }

    static class FlushTask {
        final String token;
        final long timestamp;
        final long position;

        private FlushTask(String token, long timestamp, long position) {
            this.token = token;
            this.timestamp = timestamp;
            this.position = position;
        }

    }
}
