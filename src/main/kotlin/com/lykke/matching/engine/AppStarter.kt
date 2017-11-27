package com.lykke.matching.engine

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.matching.engine.socket.SocketServer
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.HttpConfigParser
import com.lykke.matching.engine.utils.migration.ReservedVolumesRecalculator
import com.lykke.utils.AppInitializer
import com.lykke.utils.AppVersion
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import com.lykke.utils.alivestatus.processor.AliveStatusProcessorFactory
import org.apache.log4j.Logger

val LOGGER = Logger.getLogger("AppStarter")

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        LOGGER.error("Not enough args. Usage: httpConfigString")
        return
    }

    AppInitializer.init()

    val config = HttpConfigParser.initConfig(args[0])

    if (config.me.migrate) {
        ReservedVolumesRecalculator().recalculate(config)
        return
    }

    MetricsLogger.init(config.slackNotifications.azureQueue.connectionString,
            config.slackNotifications.azureQueue.queueName,
            config.slackNotifications.throttlingLimitSeconds)

    ThrottlingLogger.init(config.throttlingLogger)

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
    Runtime.getRuntime().addShutdownHook(ShutdownHook(config))

    SocketServer(config) { appInitialData ->
        MetricsLogger.getLogger().logWarning("Spot.${config.me.name} ${AppVersion.VERSION} : Started : ${appInitialData.ordersCount} orders, ${appInitialData.balancesCount} balances for ${appInitialData.clientsCount} clients")
    }.run()
}

internal class ShutdownHook(private val config: Config) : Thread() {
    init {
        this.name = "ShutdownHook"
    }

    override fun run() {
        LOGGER.info("Stopping application")

        MetricsLogger.logWarning("Spot.${config.me.name} ${AppVersion.VERSION} : Stopped :")
    }
}