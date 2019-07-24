package io.joshworks.fstore.lsmtree.sstable;

public interface TreeFunctions<K extends Comparable<K>, V> {

    /**
     * Returns the greatest element in this set less than or equal to
     * the given element, or {@code null} if there is no such element.
     *
     * @param key the value to match
     * @return the greatest element less than or equal to {@code key},
     * or {@code null} if there is no such element
     */
    Entry<K, V> floor(K key);

    /**
     * Returns the least element in this set greater than or equal to
     * the given element, or {@code null} if there is no such element.
     *
     * @param key the value to match
     * @return the least element greater than or equal to {@code key},
     * or {@code null} if there is no such element
     */
    Entry<K, V> ceiling(K key);


    /**
     * Returns the least element in this set strictly greater than the
     * given element, or {@code null} if there is no such element.
     *
     * @param key the value to match
     * @return the least element greater than {@code key},
     * or {@code null} if there is no such element
     */
    Entry<K, V> higher(K key);


    /**
     * Returns the greatest element in this set strictly less than the
     * given element, or {@code null} if there is no such element.
     *
     * @param key the value to match
     * @return the greatest element less than {@code key},
     * or {@code null} if there is no such element
     */
    Entry<K, V> lower(K key);


    /**
     * Return the entry has the same key as the provided one, null if it doesn't exist.
     *
     * @param key the value to match
     * @return the element equal to {@code key},
     * or {@code null} if there is no such element
     */
    Entry<K, V> get(K key);
}
