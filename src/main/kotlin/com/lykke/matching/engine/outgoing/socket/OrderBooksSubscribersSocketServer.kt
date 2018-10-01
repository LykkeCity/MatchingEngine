package com.lykke.matching.engine.outgoing.socket

import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.core.task.AsyncTaskExecutor
import org.springframework.stereotype.Component
import java.net.ServerSocket
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.PostConstruct

@Component
class OrderBooksSubscribersSocketServer(val config: Config,
                                        private val connectionsHolder: ConnectionsHolder,
                                        val genericLimitOrderService: GenericLimitOrderService,
                                        val assetsHolder: AssetsHolder,
                                        val assetsPairsHolder: AssetsPairsHolder,
                                        private val orderBookSubscribersThreadPool: AsyncTaskExecutor): Thread(OrderBooksSubscribersSocketServer::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(OrderBooksSubscribersSocketServer::class.java.name)
    }

    @PostConstruct
    private fun init() {
        if (config.me.serverOrderBookPort == null) {
            return
        }

        this.start()
    }

    override fun run() {
        val port = config.me.serverOrderBookPort
        val socket = ServerSocket(port!!)
        LOGGER.info("Waiting connection on port: $port.")
        try {

            while (true) {
                val clientConnection = socket.accept()
                val connection = Connection(clientConnection, LinkedBlockingQueue<OrderBook>(),
                        genericLimitOrderService.getAllOrderBooks(), assetsHolder, assetsPairsHolder)
                orderBookSubscribersThreadPool.submit(connection)
                connectionsHolder.addConnection(connection)
            }
        } catch (exception: Exception) {
            LOGGER.error("Got exception: ", exception)
        } finally {
            socket.close()
        }
    }
}