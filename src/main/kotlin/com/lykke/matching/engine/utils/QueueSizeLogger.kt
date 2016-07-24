package com.lykke.matching.engine.utils

import com.lykke.matching.engine.messages.MessageWrapper
import org.apache.log4j.Logger
import java.util.Queue

class QueueSizeLogger(val queue: Queue<MessageWrapper>) {
    companion object {
        val LOGGER = Logger.getLogger(QueueSizeLogger::class.java.name)
    }

    fun log() {
        LOGGER.info("Incoming queue size: ${queue.size}")
    }
}