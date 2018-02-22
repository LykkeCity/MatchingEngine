package com.lykke.matching.engine

import com.lykke.matching.engine.utils.config.ApplicationProperties

object AppContext {

    private var initialized = false

    private var properties: ApplicationProperties? = null

    fun init(properties: ApplicationProperties) {
        synchronized(this) {
            if (initialized) {
                throw IllegalStateException("AppContext is already initialized")
            }
            this.properties = properties
            initialized = true
        }
    }

    fun destroy() {
        synchronized(this) {
            if (!initialized) {
                throw IllegalStateException("AppContext is not initialized")
            }
            properties = null
            initialized = false
        }
    }

    fun properties(): ApplicationProperties = properties!!
}