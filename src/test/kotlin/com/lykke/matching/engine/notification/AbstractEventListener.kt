package com.lykke.matching.engine.notification

import java.util.concurrent.BlockingQueue

abstract class AbstractEventListener<V> {

    abstract fun getProcessingQueue(): BlockingQueue<*>

    fun getCount(): Int {
        return getProcessingQueue().size
    }

    fun getQueue(): BlockingQueue<V> {
        return getProcessingQueue() as BlockingQueue<V>
    }

    fun clear() {
        getProcessingQueue().clear()
    }
}