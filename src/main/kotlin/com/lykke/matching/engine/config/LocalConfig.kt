package com.lykke.matching.engine.config

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.config.ConfigInitializer
import org.springframework.beans.factory.FactoryBean
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("dev")
class LocalConfig : FactoryBean<Config> {
    override fun getObject(): Config {
        return ConfigInitializer.initConfig("local",  classOfT = Config::class.java)
    }

    override fun getObjectType(): Class<*> {
        return Config::class.java
    }
}