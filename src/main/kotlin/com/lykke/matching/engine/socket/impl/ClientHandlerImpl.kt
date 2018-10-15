package com.lykke.matching.engine.socket.impl

import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.socket.ClientHandler
import com.lykke.matching.engine.socket.ClientsRequestsSocketServer
import com.lykke.matching.engine.utils.IntUtils
import org.apache.log4j.Logger
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.time.LocalDateTime

class ClientHandlerImpl(
        private val messageRouter: MessageRouter,
        private val socket: Socket,
        private val socketServer: ClientsRequestsSocketServer): Thread(ClientHandlerImpl::class.java.name), ClientHandler {

    companion object {
        val LOGGER = Logger.getLogger(ClientHandlerImpl::class.java.name)
    }

    @Volatile
    private var incomingSize: Long = 0
    @Volatile
    private var outgoingSize: Long = 0
    @Volatile
    private var lastMessageAt: LocalDateTime = LocalDateTime.now()

    override var clientHostName = socket.inetAddress.hostAddress

    var inputStream: DataInputStream? = null
    var outputStream: DataOutputStream? = null

    override fun run() {
        try {
            Thread.currentThread().name = Thread.currentThread().name + "-$clientHostName"
            inputStream = DataInputStream(BufferedInputStream(socket.inputStream))
            outputStream = DataOutputStream(BufferedOutputStream(socket.outputStream))
            outputStream!!.flush()

            readMessage(inputStream!!, outputStream!!)
            LOGGER.info("Got connection from $clientHostName.")
            while (true) {
                readMessage(inputStream!!, outputStream!!)
            }
        } catch (e: Exception) {
            if (incomingSize > 0) {
                LOGGER.info("Connection from $clientHostName disconnected. [${e.message}]")
            }
            if (socket.isConnected && !socket.isClosed) {
                socket.close()
            }
            socketServer.disconnect(this)
        } finally {
            if (incomingSize > 0) {
                LOGGER.info("Connection from $clientHostName closed.")
            }
        }
    }

    private fun readMessage(inputStream: DataInputStream, outputStream: DataOutputStream) {
        lastMessageAt = LocalDateTime.now()

        val type = inputStream.readByte()
        if (type == MessageType.PING.type) {
            incomingSize++
            outgoingSize++
            LOGGER.debug("Got ping request from $clientHostName.")
            //do not read, send back ping
            outputStream.write(byteArrayOf(MessageType.PING.type))
            outputStream.flush()
            return
        }

        val sizeArray = ByteArray(4)
        inputStream.readFully(sizeArray, 0, 4)
        val size = IntUtils.little2big(sizeArray)
        val serializedData = ByteArray(size)
        inputStream.readFully(serializedData, 0, size)
        incomingSize += 1 + size
        messageRouter.process(MessageWrapper(clientHostName, type, serializedData, this))
    }

    override fun writeOutput(byteArray: ByteArray) {
        if (socket.isConnected && !socket.isClosed) {
            outgoingSize += byteArray.size
            outputStream!!.write(byteArray)
            outputStream!!.flush()
        }
    }

    override fun isConnected(): Boolean {
        val now = LocalDateTime.now()
        return now.minusMinutes(1).isBefore(lastMessageAt)
    }

    override fun disconnect() {
        try {
            socket.close()
        } catch (e: Exception) {
            LOGGER.info("Unable to close connection to $clientHostName")
        }
    }

    override fun toString(): String {
        return "Client handler, (clientHostName: $clientHostName)"
    }
}