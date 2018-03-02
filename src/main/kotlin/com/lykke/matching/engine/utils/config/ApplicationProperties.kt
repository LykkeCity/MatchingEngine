package com.lykke.matching.engine.utils.config

import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import com.lykke.matching.engine.database.cache.DataCache
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.fixedRateTimer

class ApplicationProperties(private val configDatabaseAccessor: ConfigDatabaseAccessor,
                            updateInterval: Long? = null) : DataCache() {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(ApplicationProperties::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val PROP_NAME_REJECT_INVALID_PRICE = "REJECT_INVALID_PRICE"
    }

    private var properties: Map<String, String> = emptyMap()
    private val lock = ReentrantLock()

    override fun update() {
        configDatabaseAccessor.loadConfigs()?.let { properties = it }
    }

    private fun lockUpdate() {
        lock.lock()
        try {
            update()
        } finally {
            lock.unlock()
        }
    }

    val rejectInvalidPrice: Boolean
        get() = getBoolean(PROP_NAME_REJECT_INVALID_PRICE, false)

    private fun getBoolean(name: String, defaultValue: Boolean? = null): Boolean {
        val value = properties[name]
        try {
            if (value == null && defaultValue != null) {
                return defaultValue
            }
            return value!!.toBoolean()
        } catch (e: Exception) {
            val message = "Incorrect boolean property value (name=$name, value=$value)"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            throw e
        }
    }

    private fun setBoolean(name: String, value: Boolean) = setValue(name, value.toString())

    private fun setValue(name: String, value: String) {
        lock.lock()
        try {
            configDatabaseAccessor.saveValue(name, value)
            update()
        } finally {
            lock.unlock()
        }
    }

    init {
        lockUpdate()
        if (updateInterval != null) {
            fixedRateTimer(name = "Application Properties Updater", initialDelay = updateInterval, period = updateInterval) {
                lockUpdate()
            }
        }
    }
}