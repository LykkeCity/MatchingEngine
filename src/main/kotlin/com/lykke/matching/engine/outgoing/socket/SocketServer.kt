package com.lykke.matching.engine.outgoing.socket

import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.net.ServerSocket
import java.util.concurrent.LinkedBlockingQueue

class SocketServer(val config: Config,
                   val connectionsHolder: ConnectionsHolder,
                   val genericLimitOrderService: GenericLimitOrderService,
                   val assetsHolder: AssetsHolder,
                   val assetsPairsHolder: AssetsPairsHolder,
                   private val clientRequestThreadPool: ThreadPoolTaskExecutor): Thread(SocketServer::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(SocketServer::class.java.name)
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
                clientRequestThreadPool.submit(connection)
                connectionsHolder.addConnection(connection)
            }
        } catch (exception: Exception) {
            LOGGER.error("Got exception: ", exception)
        } finally {
            socket.close()
        }
    }
}