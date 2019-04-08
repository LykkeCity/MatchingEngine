package com.lykke.matching.engine.utils.monitoring

import com.lykke.matching.engine.socket.ClientsRequestsSocketServer
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Component

@Component
class ClientConnectionsCountLogger(private val clientRequestThreadPool: ThreadPoolTaskExecutor,
                                   private val clientsRequestsSocketServer: ClientsRequestsSocketServer) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ClientConnectionsCountLogger::class.java.name)
    }

    @Scheduled(fixedRateString = "\${client.connections.count.logger.interval}")
    fun logConnectionsCount() {
        LOGGER.info("Active socket client connections count: ${clientsRequestsSocketServer.getConnectionsCount()}, " +
                "active threads size: ${clientRequestThreadPool.activeCount}")
    }
}