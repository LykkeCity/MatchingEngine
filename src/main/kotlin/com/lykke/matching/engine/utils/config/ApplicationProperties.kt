package com.lykke.matching.engine.utils.config

import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import com.lykke.matching.engine.database.cache.DataCache
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import kotlin.concurrent.fixedRateTimer

class ApplicationProperties(private val configDatabaseAccessor: ConfigDatabaseAccessor,
                            updateInterval: Long) : DataCache() {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(ApplicationProperties::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
        const val PROP_NAME_PRICE_DIFFERENCE_THRESHOLD = "PRICE_DIFFERENCE_THRESHOLD"
    }

    private var properties: Map<String, String> = emptyMap()

    override fun update() {
        configDatabaseAccessor.loadConfigs()?.let { properties = it }
    }

    val priceDifferenceThreshold: Double
        get() = getDouble(PROP_NAME_PRICE_DIFFERENCE_THRESHOLD)

    private fun getDouble(name: String): Double {
        val value = properties[name]
        try {
            return value!!.toDouble()
        } catch (e: Exception) {
            val message = "Incorrect property value (name=$name, value=$value)"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            throw e
        }
    }

    init {
        update()
        fixedRateTimer(name = "Application Properties Updater", initialDelay = updateInterval, period = updateInterval) {
            update()
        }
    }
}