package com.lykke.matching.engine

import com.lykke.matching.engine.config.HttpConfigParser
import com.lykke.matching.engine.config.LocalConfig
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import com.lykke.utils.alivestatus.processor.AliveStatusProcessorFactory
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.env.Environment

class AppStatusStarter : ApplicationContextInitializer<GenericApplicationContext> {
    override fun initialize(applicationContext: GenericApplicationContext) {

        val azureStatusProcessor = getAzureStatusProcessor(getConfig(applicationContext.environment))

        try {
            azureStatusProcessor.run()
        } catch (e: CheckAppInstanceRunningException) {
            LOGGER.error(e.message)
            System.exit(1)
        }
    }

    private fun getConfig(environment: Environment): Config {
        return if(environment.activeProfiles.contains("local_config")) {
            LocalConfig().getObject()
        } else {
            HttpConfigParser(environment).getObject()
        }
    }

    private fun getAzureStatusProcessor(config: Config): Runnable {
        return AliveStatusProcessorFactory
                .createAzureProcessor(connectionString = config.me.db.matchingEngineConnString,
                        appName = config.me.name,
                        config = config.me.aliveStatus)
    }
}