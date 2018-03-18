package com.lykke.matching.engine

import com.lykke.matching.engine.database.azure.AzureConfigDatabaseAccessor
import com.lykke.matching.engine.utils.balance.correctReservedVolumesIfNeed
import com.lykke.matching.engine.utils.config.ApplicationProperties
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import com.lykke.utils.alivestatus.processor.AliveStatusProcessorFactory
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

val LOGGER = Logger.getLogger("AppStarter")

@SpringBootApplication
open class AppStarter {

    @Autowired
    private lateinit var config: Config

    @Autowired
    lateinit var socketServer: Runnable

    fun main(args: Array<String>) {
        if (args.isEmpty()) {
            LOGGER.error("Not enough args. Usage: httpConfigString")
            return
        }

        val context = SpringApplication.run(AppStarter::class.java, *args)

        socketServer.run()

        context.close()


        val properties = ApplicationProperties(AzureConfigDatabaseAccessor(config.me.db.matchingEngineConnString), 60000)
        AppContext.init(properties)

        try {
            AliveStatusProcessorFactory
                    .createAzureProcessor(connectionString = config.me.db.matchingEngineConnString,
                            appName = "MatchingEngine",
                            config = config.me.aliveStatus)
                    .run()
        } catch (e: CheckAppInstanceRunningException) {
            LOGGER.error(e.message)
            return
        }

        MetricsLogger.init("ME", config.slackNotifications)

        ThrottlingLogger.init(config.throttlingLogger)

        correctReservedVolumesIfNeed(config)
        Runtime.getRuntime().addShutdownHook(ShutdownHook(config))

    }

}


internal class ShutdownHook(private val config: Config) : Thread() {
    init {
        this.name = "ShutdownHook"
    }

    override fun run() {
        LOGGER.info("Stopping application")
        AppContext.destroy()
        MetricsLogger.logWarning("Spot.${config.me.name} ${AppVersion.VERSION} : Stopped :")
    }
}