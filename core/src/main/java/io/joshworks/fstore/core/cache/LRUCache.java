/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.joshworks.fstore.core.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A non-blocking cache where entries are indexed by a key.
 * <p>
 * <p>To reduce contention, entry allocation and eviction execute in a sampling
 * fashion (entry hits modulo N). Eviction follows an LRU approach (oldest sampled
 * entries are removed first) when the cache is out of capacity.</p>
 * <p>
 * <p>
 * This cache can also be configured to run in FIFO mode, rather than LRU.
 *
 * @author Jason T. Greene
 * @author Stuart Douglas
 */
class LRUCache<K, V> implements Cache<K, V> {
    private static final int SAMPLE_INTERVAL = 5;

    /**
     * Max active entries that are present in the cache.
     */
    private final int maxEntries;

    private final ConcurrentMap<K, CacheEntry<K, V>> cache;
    private final ConcurrentDirectDeque<CacheEntry<K, V>> accessQueue;
    /**
     * How long an item can stay in the cache in milliseconds
     */
    private final int maxAge;
    private final boolean fifo;

    LRUCache(int maxEntries, final int maxAgeSec) {
        this(maxEntries, maxAgeSec, false);
    }

    LRUCache(int maxEntries, final int maxAgeSec, boolean fifo) {
        this.maxAge = maxAgeSec > 0 ? maxAgeSec * 1000 : -1;
        this.cache = new ConcurrentHashMap<>(maxEntries);
        this.accessQueue = ConcurrentDirectDeque.newInstance();
        this.maxEntries = maxEntries;
        this.fifo = fifo;
    }

    @Override
    public void add(K key, V newValue) {
        CacheEntry<K, V> value = cache.get(key);
        if (value == null) {
            long expires = maxAge;
            if (maxAge > 0) {
                expires = System.currentTimeMillis() + maxAge;
            }
            value = new CacheEntry<>(key, newValue, expires);
            CacheEntry<K, V> result = cache.putIfAbsent(key, value);
            if (result != null) {
                value = result;
                value.setValue(newValue);
            }
            bumpAccess(value);
            if (cache.size() > maxEntries) {
                //remove the oldest
                CacheEntry<K, V> oldest = accessQueue.poll();
                if (oldest != null && oldest != value) {
                    this.remove(oldest.key());
                }
            }
        } else {
            value.value = newValue;
            bumpAccess(value);
        }
    }

    @Override
    public V get(K key) {
        CacheEntry<K, V> cacheEntry = cache.get(key);
        if (cacheEntry == null) {
            return null;
        }
        long expires = cacheEntry.getExpires();
        if (expires >= 0) {
            if (System.currentTimeMillis() > expires) {
                remove(key);
                return null;
            }
        }

        if (!fifo) {
            if (cacheEntry.hit() % SAMPLE_INTERVAL == 0) {
                bumpAccess(cacheEntry);
            }
        }

        return cacheEntry.getValue();
    }

    private void bumpAccess(CacheEntry<K, V> cacheEntry) {
        Object prevToken = cacheEntry.claimToken();
        if (!Boolean.FALSE.equals(prevToken)) {
            if (prevToken != null) {
                accessQueue.removeToken(prevToken);
            }

            Object token = null;
            try {
                token = accessQueue.offerLastAndReturnToken(cacheEntry);
            } catch (Throwable t) {
                // In case of disaster (OOME), we need to release the claim, so leave it as null
            }

            if (!cacheEntry.setToken(token) && token != null) { // Always set if null
                accessQueue.removeToken(token);
            }
        }
    }

    @Override
    public V remove(K key) {
        CacheEntry<K, V> remove = cache.remove(key);
        if (remove != null) {
            Object old = remove.clearToken();
            if (old != null) {
                accessQueue.removeToken(old);
            }
            return remove.getValue();
        } else {
            return null;
        }
    }

    @Override
    public long size() {
        return cache.size();
    }

    @Override
    public void clear() {
        cache.clear();
        accessQueue.clear();
    }

    public static final class CacheEntry<K, V> {

        private static final Object CLAIM_TOKEN = new Object();

        private static final AtomicIntegerFieldUpdater<CacheEntry> hitsUpdater = AtomicIntegerFieldUpdater.newUpdater(CacheEntry.class, "hits");

        private static final AtomicReferenceFieldUpdater<CacheEntry, Object> tokenUpdator = AtomicReferenceFieldUpdater.newUpdater(CacheEntry.class, Object.class, "accessToken");

        private final K key;
        private final long expires;
        private volatile V value;
        private volatile int hits = 1;
        private volatile Object accessToken;

        private CacheEntry(K key, V value, final long expires) {
            this.key = key;
            this.value = value;
            this.expires = expires;
        }

        public V getValue() {
            return value;
        }

        public void setValue(final V value) {
            this.value = value;
        }

        public int hit() {
            for (; ; ) {
                int i = hits;

                if (hitsUpdater.weakCompareAndSet(this, i, ++i)) {
                    return i;
                }

            }
        }

        public K key() {
            return key;
        }

        Object claimToken() {
            for (; ; ) {
                Object current = this.accessToken;
                if (current == CLAIM_TOKEN) {
                    return Boolean.FALSE;
                }

                if (tokenUpdator.compareAndSet(this, current, CLAIM_TOKEN)) {
                    return current;
                }
            }
        }

        boolean setToken(Object token) {
            return tokenUpdator.compareAndSet(this, CLAIM_TOKEN, token);
        }

        Object clearToken() {
            Object old = tokenUpdator.getAndSet(this, null);
            return old == CLAIM_TOKEN ? null : old;
        }

        public long getExpires() {
            return expires;
        }
    }
}
