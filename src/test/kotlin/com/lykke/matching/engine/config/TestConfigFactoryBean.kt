package com.lykke.matching.engine.config

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.config.ConfigInitializer
import org.springframework.beans.factory.FactoryBean

class TestConfigFactoryBean : FactoryBean<Config> {
    private val trustedClients: MutableSet<String> = HashSet()

    override fun getObject(): Config {
        val initConfig = ConfigInitializer.initConfig("local", classOfT = Config::class.java)

        val trustedClients = initConfig.me.trustedClients as MutableSet
        trustedClients.addAll(this.trustedClients)

        return initConfig
    }

    override fun getObjectType(): Class<*> {
        return Config::class.java
    }

    fun addTrustedClient(trustedClient : String) {
        trustedClients.add(trustedClient)
    }
}