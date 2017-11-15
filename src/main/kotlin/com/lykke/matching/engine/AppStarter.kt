package com.lykke.matching.engine

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.matching.engine.socket.SocketServer
import com.lykke.matching.engine.utils.AppVersion
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.HttpConfigParser
import com.lykke.matching.engine.utils.migration.ReservedVolumesRecalculator
import com.lykke.utils.alivestatus.database.AliveStatusDatabaseAccessor
import com.lykke.utils.alivestatus.database.azure.AzureAliveStatusDatabaseAccessor
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import org.apache.log4j.Logger
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val LOGGER = Logger.getLogger("AppStarter")

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        LOGGER.error("Not enough args. Usage: httpConfigString")
        return
    }

    val startTime = LocalDateTime.now()

    teeLog("Application launched at " + startTime.format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")))
    teeLog("Revision-number: " + AppVersion.REVISION_NUMBER)
    teeLog("Build-number: " + AppVersion.BUILD_NUMBER)
    teeLog("Version: " + AppVersion.VERSION)
    teeLog("Working-dir: " + File(".").absolutePath)
    teeLog("Java-Info: " + System.getProperty("java.vm.name") + " (" + System.getProperty("java.version") + ")")

    val config = HttpConfigParser.initConfig(args[0])

    if (config.me.migrate) {
        ReservedVolumesRecalculator().recalculate(config)
        return
    }

    MetricsLogger.init(config.slackNotifications.azureQueue.connectionString,
            config.slackNotifications.azureQueue.queueName,
            config.slackNotifications.throttlingLimitSeconds)

    ThrottlingLogger.init(config.throttlingLogger)

    val aliveStatusDatabaseAccessor = AzureAliveStatusDatabaseAccessor(config.me.db.matchingEngineConnString, "MatchingEngine")
    try {
        aliveStatusDatabaseAccessor.checkAndLock()
    } catch (e: CheckAppInstanceRunningException) {
        LOGGER.error(e.message)
        return
    }
    Runtime.getRuntime().addShutdownHook(ShutdownHook(config, aliveStatusDatabaseAccessor))

    SocketServer(config) { appInitialData ->
        MetricsLogger.getLogger().logWarning("Spot.${config.me.name} ${AppVersion.VERSION} : Started : ${appInitialData.ordersCount} orders, ${appInitialData.balancesCount} balances for ${appInitialData.clientsCount} clients")
    }.run()
}

private fun teeLog(message: String) {
    println(message)
    LOGGER.info(message)
}

internal class ShutdownHook(private val config: Config, private val aliveStatusDatabaseAccessor: AliveStatusDatabaseAccessor) : Thread() {
    init {
        this.name = "ShutdownHook"
    }

    override fun run() {
        LOGGER.info("Stopping application")

        MetricsLogger.logWarning("Spot.${config.me.name} ${AppVersion.VERSION} : Stopped :")

        aliveStatusDatabaseAccessor.unlock()
    }
}