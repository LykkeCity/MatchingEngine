package com.lykke.matching.engine.notification

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractEventListener<E, V> {
    private val eventQueue: BlockingQueue<V> = LinkedBlockingQueue()

    open fun process(event: E) {
        eventQueue.add(extract(event))
    }

    fun getCount(): Int {
        return eventQueue.size
    }

    fun getQueue(): BlockingQueue<V> {
        return eventQueue
    }

    fun clear() {
        eventQueue.clear()
    }

    abstract fun extract(t: E): V
}