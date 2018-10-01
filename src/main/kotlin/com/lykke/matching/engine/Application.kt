package com.lykke.matching.engine

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class Application {
    @Autowired
    lateinit var clientRequestSocketServer: Runnable

    fun run () {
        clientRequestSocketServer.run()
    }
}