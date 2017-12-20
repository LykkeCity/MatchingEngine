package com.lykke.matching.engine

import com.lykke.matching.engine.socket.SocketServer
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.HttpConfigParser
import com.lykke.matching.engine.utils.balance.correctReservedVolumesIfNeed
import com.lykke.utils.AppInitializer
import com.lykke.utils.AppVersion
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import com.lykke.utils.alivestatus.processor.AliveStatusProcessorFactory
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.log4j.Logger

val LOGGER = Logger.getLogger("AppStarter")

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        LOGGER.error("Not enough args. Usage: httpConfigString")
        return
    }

    AppInitializer.init()

    val config = HttpConfigParser.initConfig(args[0])

    MetricsLogger.init("ME", config.slackNotifications)

    ThrottlingLogger.init(config.throttlingLogger)

    correctReservedVolumesIfNeed(config)

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