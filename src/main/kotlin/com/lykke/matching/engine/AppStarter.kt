package com.lykke.matching.engine

import com.lykke.matching.engine.logging.HttpLogger
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.LoggableObject
import com.lykke.matching.engine.logging.ME_STATUS
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.socket.SocketServer
import org.apache.log4j.Logger
import java.util.concurrent.LinkedBlockingQueue

val LOGGER = Logger.getLogger("AppStarter")

fun main(args: Array<String>) {
    if (args.size == 0) {
        LOGGER.error("Config file is not provided, stopping application")
        return
    }
    val config = loadLocalConfig(args[0])

    MetricsLogger.init(config.getProperty("metric.logger.key.value"), config.getProperty("metric.logger.line"))

    Runtime.getRuntime().addShutdownHook(ShutdownHook(config.getProperty("metric.logger.key.value")))

    SocketServer(config).run()
}

internal class ShutdownHook(val link: String) : Thread() {

    init {
        this.name = "ShutdownHook"
    }

    override fun run() {
        LOGGER.info("Stoppping application")
        HttpLogger(link, LinkedBlockingQueue<LoggableObject>()).sendHttpRequest(KeyValue(ME_STATUS, "False"))
    }
}