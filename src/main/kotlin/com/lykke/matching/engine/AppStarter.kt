package com.lykke.matching.engine

import com.lykke.matching.engine.utils.balance.correctReservedVolumesIfNeed
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.core.env.SimpleCommandLinePropertySource

val LOGGER = Logger.getLogger("AppStarter")

@SpringBootApplication
open class AppStarter {

    @Autowired
    private lateinit var config: Config

    @Autowired
    lateinit var socketServer: Runnable

    @Autowired
    lateinit var azureStatusProcessor: Runnable

    @Autowired
    lateinit var applicationContext: ApplicationContext

    fun main(args: Array<String>) {
        if (args.isEmpty()) {
            LOGGER.error("Not enough args. Usage: httpConfigString")
            return
        }
        val context = SpringApplication.run(AppStarter::class.java, *args)
        addCommandLinePropertySource(args, context)

        try {
            azureStatusProcessor.run()
        } catch (e: CheckAppInstanceRunningException) {
            LOGGER.error(e.message)
            return
        }

        correctReservedVolumesIfNeed(config, applicationContext)
        socketServer.run()
    }

    private fun addCommandLinePropertySource(args: Array<String>, context: ConfigurableApplicationContext) {
        val commandLineArguments = SimpleCommandLinePropertySource(*args)
        context
                .environment
                .propertySources
                .addFirst(commandLineArguments)
    }
}


