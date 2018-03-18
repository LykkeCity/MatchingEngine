package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.socket.SocketServer
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppInitializer
import com.lykke.utils.AppVersion
import com.lykke.utils.logging.MetricsLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.annotation.PostConstruct

@Configuration
class AppConfiguration {

    @Autowired
    private lateinit var config: Config

    @Bean
    fun socketServer(): Runnable {
        return SocketServer { appInitialData ->
            MetricsLogger.getLogger().logWarning("""Spot.${config.me.name} ${AppVersion.VERSION}
                |: Started : ${appInitialData.ordersCount} orders, ${appInitialData.balancesCount}
                |balances for ${appInitialData.clientsCount} clients""".trimMargin())
        }
    }

    @PostConstruct
    fun init() {
        AppInitializer.init()
    }
}