package com.lykke.matching.engine

import com.lykke.matching.engine.database.cache.ApplicationSettingsCache

object AppContext {

    private var initialized = false

    private var settingsCache: ApplicationSettingsCache? = null

    fun init(settingsCache: ApplicationSettingsCache) {
        synchronized(this) {
            if (initialized) {
                throw IllegalStateException("AppContext is already initialized")
            }
            this.settingsCache = settingsCache
            initialized = true
        }
    }

    fun destroy() {
        synchronized(this) {
            if (!initialized) {
                throw IllegalStateException("AppContext is not initialized")
            }
            settingsCache = null
            initialized = false
        }
    }

    fun properties(): ApplicationSettingsCache = settingsCache!!
}