package com.lykke.matching.engine

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class Application {
    @Autowired
    lateinit var clientsRequestsSocketServer: Runnable

    fun run () {
        clientsRequestsSocketServer.run()
    }
}