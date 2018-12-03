package com.lykke.matching.engine.socket

import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.messages.MessageProcessor
import com.lykke.matching.engine.socket.impl.ClientHandlerImpl
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Component
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.RejectedExecutionException
import java.util.regex.Pattern

@Component
class ClientsRequestsSocketServer(private val clientRequestThreadPool: ThreadPoolTaskExecutor) : Runnable {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Autowired
    private lateinit var messageRouter: MessageRouter

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(ClientsRequestsSocketServer::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val connections = CopyOnWriteArraySet<ClientHandler>()

    override fun run() {
        val messageProcessor = MessageProcessor(messageRouter, applicationContext)

        messageProcessor.start()

        val appInitialData = messageProcessor.appInitialData

        MetricsLogger.getLogger().logWarning("Spot.${config.me.name} ${AppVersion.VERSION} : " +
                "Started : ${appInitialData.ordersCount} orders, ${appInitialData.stopOrdersCount} " +
                "stop orders,${appInitialData.balancesCount} " +
                "balances for ${appInitialData.clientsCount} clients")

        val port = config.me.socket.port
        val socket = ServerSocket(port)
        LOGGER.info("Waiting connection on port: $port.")
        try {
            while (true) {
                submitClientConnection(socket.accept())
            }
        } catch (exception: Exception) {
            LOGGER.error("Got exception: ", exception)
            METRICS_LOGGER.logError("Fatal exception", exception)
        } finally {
            socket.close()
        }
    }

    fun submitClientConnection(clientConnection: Socket) {
        if (isConnectionAllowed(getWhiteList(), clientConnection.inetAddress.hostAddress)) {
            val handler = ClientHandlerImpl(messageRouter, clientConnection, this)
            try {
                clientRequestThreadPool.submit(handler)
            } catch (e: RejectedExecutionException) {
                logPoolRejection(handler)
                closeClientConnection(clientConnection)
                return
            }
            connect(handler)
        } else {
            closeClientConnection(clientConnection)
            LOGGER.info("Connection from host ${clientConnection.inetAddress.hostAddress} is not allowed.")
        }
    }


    fun logPoolRejection(rejectedClientHandler: ClientHandler) {
        val message = "Task rejected from client handler thread pool, client can not be connected to ME, " +
                "rejected tasks: [$rejectedClientHandler] " +
                "active threads size ${clientRequestThreadPool.activeCount}, " +
                "max pool size ${clientRequestThreadPool.maxPoolSize}"

        METRICS_LOGGER.logError(message)
        LOGGER.error(message)
    }

    private fun closeClientConnection(clientConnection: Socket) {
        try {
            clientConnection.close()
        } catch (e: Exception) {
            LOGGER.error("Error during connection close for connection: ${clientConnection.inetAddress.hostAddress}")
        }
    }

    private fun isConnectionAllowed(whitelist: List<String>?, host: String): Boolean {
        if (whitelist != null) {
            whitelist.forEach {
                if (Pattern.compile(it).matcher(host).matches()) {
                    return true
                }
            }
            return false
        }
        return true
    }

    private fun getWhiteList(): List<String>? {
        return config.me.whiteList?.split(";")
    }

    private fun connect(handler: ClientHandler) {
        connections.add(handler)
    }

    fun disconnect(handler: ClientHandler) {
        connections.remove(handler)
    }
}