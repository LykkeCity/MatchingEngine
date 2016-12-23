package com.lykke.matching.engine

import com.lykke.matching.engine.logging.HttpLogger
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.LoggableObject
import com.lykke.matching.engine.logging.ME_STATUS
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.socket.SocketServer
import com.lykke.matching.engine.utils.AppVersion
import org.apache.log4j.Logger
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.LinkedBlockingQueue

val LOGGER = Logger.getLogger("AppStarter")

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        LOGGER.error("Config file is not provided, stopping application")
        return
    }

    teeLog("Application launched at " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")))
    teeLog("Revision-number: " + AppVersion.REVISION_NUMBER)
    teeLog("Build-number: " + AppVersion.BUILD_NUMBER)
    teeLog("Version: " + AppVersion.VERSION)
    teeLog("Working-dir: " + File(".").absolutePath)
    teeLog("Java-Info: " + System.getProperty("java.vm.name") + " (" + System.getProperty("java.version") + ")")

    val config = loadLocalConfig(args[0])
    val azureConfig = loadConfig(config)
    val dbConfig = azureConfig["Db"] as Map<String, String>

    MetricsLogger.init(config.getProperty("metric.logger.key.value"), config.getProperty("metric.logger.line"),
            dbConfig["SharedStorageConnString"]!!, azureConfig["SlackNotificationsQueueName"] as String? ?: "app-err-notifications",
            config.getProperty("metric.logger.size").toInt())

    Runtime.getRuntime().addShutdownHook(ShutdownHook(config.getProperty("metric.logger.key.value")))

    SocketServer(config).run()
}

private fun teeLog(message: String) {
    println(message)
    LOGGER.info(message)
}

internal class ShutdownHook(val link: String) : Thread() {
    init {
        this.name = "ShutdownHook"
    }

    override fun run() {
        LOGGER.info("Stopping application")
        HttpLogger(link, LinkedBlockingQueue<LoggableObject>()).sendHttpRequest(KeyValue(ME_STATUS, "False"))
    }
}