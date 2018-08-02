package com.lykke.matching.engine

import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class Application {
    @Autowired
    lateinit var socketServer: Runnable

    @Autowired
    lateinit var azureStatusProcessor: Runnable

    @Autowired
    @Qualifier("appStarterLogger")
    lateinit var LOGGER: Logger

    fun run () {
        try {
            azureStatusProcessor.run()
        } catch (e: CheckAppInstanceRunningException) {
            LOGGER.error("Error occurred while starting application, ${e.message}", e)
            System.exit(1)
        }

        socketServer.run()
    }
}