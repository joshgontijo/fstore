package io.joshworks.es;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//just a wrapper to track usage
public class StoreLock {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public Lock readLock() {
        Lock lock = this.lock.readLock();
        lock.lock();
        return lock;
    }

    public Lock writeLock() {
        Lock lock = this.lock.writeLock();
        lock.lock();
        return lock;
    }

}
