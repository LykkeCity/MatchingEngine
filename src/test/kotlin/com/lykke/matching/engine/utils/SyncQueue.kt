package com.lykke.matching.engine.utils

import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class SyncQueue<T> : BlockingQueue<T> {

    private val queue = LinkedBlockingQueue<T>()

    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    private var prevEvent: T? = null
    private var curEvent: T? = null

    override fun containsAll(elements: Collection<T>): Boolean {
        throw NotImplementedException()
    }

    override fun contains(element: T): Boolean {
        throw NotImplementedException()
    }

    override fun addAll(elements: Collection<T>): Boolean {
        throw NotImplementedException()
    }

    override fun clear() {
        throw NotImplementedException()
    }

    override fun element(): T {
        throw NotImplementedException()
    }

    override fun take(): T {
        lock.lock()
        try {
            prevEvent = curEvent
            condition.signal()
        } finally {
            lock.unlock()
        }
        curEvent = queue.take()
        return curEvent as T
    }

    override fun removeAll(elements: Collection<T>): Boolean {
        throw NotImplementedException()
    }

    override fun add(element: T): Boolean {
        throw NotImplementedException()
    }

    override fun offer(e: T): Boolean {
        throw NotImplementedException()
    }

    override fun offer(e: T, timeout: Long, unit: TimeUnit?): Boolean {
        throw NotImplementedException()
    }

    override fun iterator(): MutableIterator<T> {
        throw NotImplementedException()
    }

    override fun peek(): T {
        throw NotImplementedException()
    }

    override fun put(e: T) {
        lock.lock()
        try {
            val event = e
            queue.put(event)
            while (prevEvent != event) {
                condition.await()
            }
        } finally {
            lock.unlock()
        }
    }

    override fun isEmpty(): Boolean {
        throw NotImplementedException()
    }

    override fun remove(element: T): Boolean {
        throw NotImplementedException()
    }

    override fun remove(): T {
        throw NotImplementedException()
    }

    override fun drainTo(c: MutableCollection<in T>?): Int {
        throw NotImplementedException()
    }

    override fun drainTo(c: MutableCollection<in T>?, maxElements: Int): Int {
        throw NotImplementedException()
    }

    override fun retainAll(elements: Collection<T>): Boolean {
        throw NotImplementedException()
    }

    override fun remainingCapacity(): Int {
        throw NotImplementedException()
    }

    override fun poll(timeout: Long, unit: TimeUnit?): T {
        throw NotImplementedException()
    }

    override fun poll(): T {
        throw NotImplementedException()
    }

    override val size: Int
        get() = throw NotImplementedException()
}