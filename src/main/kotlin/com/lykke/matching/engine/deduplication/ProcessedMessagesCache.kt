package com.lykke.matching.engine.deduplication

import java.util.Date
import java.util.HashMap
import java.util.HashSet
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.fixedRateTimer
import kotlin.concurrent.read
import kotlin.concurrent.write

class ProcessedMessagesCache(
        expirationTime: Long,
        messages: Set<ProcessedMessage>
) {
    private var prevPeriodMessageMap = HashMap<Byte, MutableSet<ProcessedMessage>>()
    private var messagesMap = HashMap<Byte, MutableSet<ProcessedMessage>>()

    private val lock = ReentrantReadWriteLock()

    fun addMessage(message: ProcessedMessage) {
        lock.write {
            messagesMap.getOrPut(message.type) { HashSet() }.add(message)
        }
    }

    fun isProcessed(type: Byte, messageId: String): Boolean {
        lock.read {
            return messagesMap[type]?.find { it.messageId == messageId } != null ||
                   prevPeriodMessageMap[type]?.find { it.messageId == messageId } != null
        }
    }

    private fun clean() {
        lock.write {
            prevPeriodMessageMap = messagesMap
            messagesMap = HashMap()
        }
    }

    init {
        val cutoffTime = Date().time - expirationTime
        messages.forEach {
            if (it.timestamp > cutoffTime) {
                messagesMap.getOrPut(it.type) { HashSet() }.add(it)
            }
        }

        fixedRateTimer(name = "ProcessedMessagesCacheCleaner", initialDelay = expirationTime, period = expirationTime) {
            clean()
        }
    }
}