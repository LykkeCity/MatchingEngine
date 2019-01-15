package com.lykke.matching.engine.config

import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

@Component("Config")
class ConfigFactory(private val environment: Environment): FactoryBean<Config> {
    companion object {
        private val LOCAL_CONFIG_PROFILE = "local_config"
        private var config: Config? = null

        @Synchronized
        fun getConfig(environment: Environment): Config {
            if (config == null) {
                config = if (environment.acceptsProfiles(LOCAL_CONFIG_PROFILE)) {
                    LocalConfigFactory.getConfig()
                } else {
                    HttpConfigFactory.getConfig(environment)
                }
            }

            return config as Config
        }
    }

    override fun getObject(): Config? {
        return getConfig(environment)
    }

    override fun getObjectType(): Class<*>? {
        return Config::class.java
    }
}