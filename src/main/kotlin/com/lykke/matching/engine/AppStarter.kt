package com.lykke.matching.engine

import org.apache.log4j.Logger

val LOGGER = Logger.getLogger("AppStarter")

fun main(args: Array<String>) {
    if (args.size == 0) {
        LOGGER.error("Config file is not provided, stopping application")
        return
    }

    val config = loadLocalConfig(args[0])
    SocketServer(config).run()
}