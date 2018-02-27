package com.lykke.matching.engine

import com.lykke.matching.engine.database.WalletsStorage
import com.lykke.matching.engine.utils.config.ApplicationProperties
import com.lykke.matching.engine.utils.config.Config

object AppContext {

    private var initialized = false
    private var walletsStorage: WalletsStorage? = null
    private var properties: ApplicationProperties? = null

    @Synchronized
    fun init(config: Config, properties: ApplicationProperties) {
        synchronized(this) {
            if (initialized) {
                throw IllegalStateException("AppContext is already initialized")
            }
            this.walletsStorage = config.me.walletsStorage
            this.properties = properties
            initialized = true
        }
    }

    @Synchronized
    fun destroy() {
        synchronized(this) {
            if (!initialized) {
                throw IllegalStateException("AppContext is not initialized")
            }
            walletsStorage = null
            properties = null
            initialized = false
        }
    }

    fun getWalletsStorage(): WalletsStorage? {
        return walletsStorage
    }

    fun properties(): ApplicationProperties = properties!!

}