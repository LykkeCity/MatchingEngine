package com.lykke.matching.engine

import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.ME_STATUS
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.socket.SocketServer
import org.apache.log4j.Logger

val LOGGER = Logger.getLogger("AppStarter")
val METRICS_LOGGER = MetricsLogger.getLogger()

fun main(args: Array<String>) {
    if (args.size == 0) {
        LOGGER.error("Config file is not provided, stopping application")
        return
    }
    val config = loadLocalConfig(args[0])

    MetricsLogger.init(config.getProperty("metric.logger.key.value"), config.getProperty("metric.logger.line"))

    Runtime.getRuntime().addShutdownHook(ShutdownHook())

    SocketServer(config).run()
}

internal class ShutdownHook() : Thread() {

    init {
        this.name = "ShutdownHook"
    }

    override fun run() {
        METRICS_LOGGER.log(KeyValue(ME_STATUS, "False"))
    }
}