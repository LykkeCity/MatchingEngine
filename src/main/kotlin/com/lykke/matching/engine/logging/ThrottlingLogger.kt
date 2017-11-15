package com.lykke.matching.engine.logging

import com.lykke.matching.engine.utils.config.ThrottlingLoggerConfig
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.concurrent.fixedRateTimer

class ThrottlingLogger private constructor(name: String) {

    companion object {
        private val LOGGER = Logger.getLogger(ThrottlingLogger::class.java.name)

        private val DEFAULT_TTL_MINUTES = 60
        private val DEFAULT_CLEANER_INTERVAL = TimeUnit.HOURS.toMillis(3)

        private var throttlingLimit: Long = 0
        private var messagesTtlMinutes = DEFAULT_TTL_MINUTES
        private var cleanerInterval = DEFAULT_CLEANER_INTERVAL

        fun init(config: ThrottlingLoggerConfig) {
            throttlingLimit = TimeUnit.SECONDS.toMillis(config.limitSeconds.toLong())
        }

        fun getLogger(name: String): ThrottlingLogger {
            return ThrottlingLogger(name)
        }
    }

    private val logger: Logger = Logger.getLogger(name)
    private val sentTimestamps = ConcurrentHashMap<String, Long>()

    fun debug(message: String) {
        logger.debug(message)
    }

    fun info(message: String) {
        logger.info(message)
    }

    fun error(message: String) {
        logger.error(message)
    }

    fun error(message: String, t: Throwable) {
        if (errorWasLoggedWithinTimeout(message)) {
            return
        }
        logger.error(message, t)
        sentTimestamps[message] = Date().time
    }

    private fun errorWasLoggedWithinTimeout(errorMessage: String): Boolean {
        val lastSentTimestamp = sentTimestamps[errorMessage]
        return lastSentTimestamp != null && lastSentTimestamp > Date().time - throttlingLimit
    }

    private fun clearSentMessageTimestamps(ttlMinutes: Int) {
        var removedItems = 0
        val threshold = Date().time - TimeUnit.MINUTES.toMillis(ttlMinutes.toLong())
        val iterator = sentTimestamps.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.value < threshold) {
                iterator.remove()
                removedItems++
            }
        }
        LOGGER.debug("Removed $removedItems from ErrorsLogger")
    }

    init {
        fixedRateTimer(name = "ThrottlingLoggerCleaner", initialDelay = cleanerInterval, period = cleanerInterval) {
            clearSentMessageTimestamps(messagesTtlMinutes)
        }
    }
}