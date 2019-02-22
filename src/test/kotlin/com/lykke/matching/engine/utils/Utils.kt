package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import org.slf4j.LoggerFactory
import java.util.*
import com.nhaarman.mockito_kotlin.any
import org.mockito.Mockito
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

fun getSetting(value: String, name: String = value) = Setting(name, value, true)

fun getExecutionContext(date: Date, executionContextFactory: ExecutionContextFactory): ExecutionContext {
    return executionContextFactory.create("test",
            "test",
            MessageType.LIMIT_ORDER,
            null,
            emptyMap(),
            date,
            LoggerFactory.getLogger(""),
            LoggerFactory.getLogger(""))
}

inline fun <reified T : Any> initSyncQueue(queueMock: BlockingQueue<T>) {

    val queue = LinkedBlockingQueue<Any>()

    val lock = ReentrantLock()
    val condition = lock.newCondition()

    var prevEvent: Any? = null
    var curEvent: Any? = null
    var consumedEvent: Any? = null

    Mockito.`when`(queueMock.put(any())).thenAnswer { invocation ->
        lock.lock()
        try {
            val event = invocation.arguments[0]
            queue.put(event)
            while (consumedEvent != event) {
                condition.await()
            }
        } finally {
            lock.unlock()
        }
    }
    Mockito.`when`(queueMock.take()).thenAnswer {
        prevEvent = curEvent
        lock.lock()
        try {
            consumedEvent = prevEvent
            condition.signal()
        } finally {
            lock.unlock()
        }
        curEvent = queue.take()
        curEvent
    }
}