package com.lykke.matching.engine

import com.lykke.matching.engine.database.WalletsStorage
import com.lykke.matching.engine.utils.config.Config

object AppContext {

    private var walletsStorage: WalletsStorage? = null
    private var initialized = false

    @Synchronized
    fun init(config: Config) {
        synchronized(this) {
            if (initialized) {
                throw IllegalStateException("AppContext is already initialized")
            }
            this.walletsStorage = config.me.walletsStorage
            initialized = true
        }
    }

    fun getWalletsStorage(): WalletsStorage? {
        return walletsStorage
    }

}