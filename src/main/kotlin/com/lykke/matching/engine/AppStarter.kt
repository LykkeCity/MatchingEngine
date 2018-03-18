package com.lykke.matching.engine

import com.lykke.matching.engine.utils.balance.correctReservedVolumesIfNeed
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
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

    @Autowired
    lateinit var azureStatusProcessor: Runnable

    fun main(args: Array<String>) {
        if (args.isEmpty()) {
            LOGGER.error("Not enough args. Usage: httpConfigString")
            return
        }

        SpringApplication.run(AppStarter::class.java, *args)

        try {
            azureStatusProcessor.run()
        } catch (e: CheckAppInstanceRunningException) {
            LOGGER.error(e.message)
            return
        }

        correctReservedVolumesIfNeed(config)

        socketServer.run()

    }
}


