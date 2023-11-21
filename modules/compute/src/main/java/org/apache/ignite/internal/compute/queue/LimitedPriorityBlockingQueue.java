/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.compute.queue;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Implementation of {@link PriorityBlockingQueue} with max size limitation.
 *
 * @param <E> The type of elements held in this queue.
 */
public class LimitedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E> {
    private final Lock lock = new ReentrantLock();
    private final Supplier<Integer> maxSize;

    /**
     * Constructor.
     *
     * @param maxSize Max queue size supplier.
     */
    public LimitedPriorityBlockingQueue(Supplier<Integer> maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Constructor.
     *
     * @param maxSize Max queue size supplier.
     * @param initialCapacity Initial queue capacity.
     */
    public LimitedPriorityBlockingQueue(Supplier<Integer> maxSize, int initialCapacity) {
        super(initialCapacity);
        this.maxSize = maxSize;
        checkInsert(initialCapacity);
    }

    /**
     * Constructor.
     *
     * @param maxSize Max queue size supplier.
     * @param initialCapacity Initial queue capacity.
     * @param comparator the comparator that will be used to order this priority queue.
     *     If {@code null}, the {@linkplain Comparable natural ordering} of the elements will be used.
     */
    public LimitedPriorityBlockingQueue(Supplier<Integer> maxSize, int initialCapacity, Comparator<? super E> comparator) {
        super(initialCapacity, comparator);
        this.maxSize = maxSize;
        checkInsert(initialCapacity);
    }

    @Override
    public boolean offer(E o) {
        lock.lock();
        try {
            checkInsert(1);
            return super.offer(o);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return maxSize.get() - size();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        lock.lock();
        try {
            checkInsert(c.size());
            return super.addAll(c);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll() {
        lock.lock();
        try {
            return super.poll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            return super.poll(timeout, unit);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E take() throws InterruptedException {
        lock.lock();
        try {
            return super.take();
        } finally {
            lock.unlock();
        }
    }



    @Override
    public E peek() {
        lock.lock();
        try {
            return super.peek();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return super.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        lock.lock();
        try {
            return super.remove(o);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        lock.lock();
        try {
            return super.contains(o);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        lock.lock();
        try {
            return super.drainTo(c, maxElements);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            super.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Object[] toArray() {
        lock.lock();
        try {
            return super.toArray();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        lock.lock();
        try {
            return super.toArray(a);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        lock.lock();
        try {
            return super.removeIf(filter);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        lock.lock();
        try {
            return super.removeAll(c);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        lock.lock();
        try {
            return super.retainAll(c);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        lock.lock();
        try {
            super.forEach(action);
        } finally {
            lock.unlock();
        }
    }

    private void checkInsert(int size) {
        Integer maxSize = this.maxSize.get();
        int currentSize = size();
        if (currentSize > maxSize - size) {
            throw new QueueOverflowException("Compute queue overflow when tried to insert " + size + " element(s) to queue. "
                    + "Current queue size " + currentSize + ". "
                    + "Max queue size is " + maxSize + ".");
        }
    }

}
