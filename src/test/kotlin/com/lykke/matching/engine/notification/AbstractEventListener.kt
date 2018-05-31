package com.lykke.matching.engine.notification

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractEventListener<T> {
    private val eventQueue: BlockingQueue<T> = LinkedBlockingQueue()

    open fun process(event: T) {
        eventQueue.add(event)
    }

    fun getCount(): Int {
        return eventQueue.size
    }

    fun getQueue(): BlockingQueue<T> {
        return eventQueue
    }
}