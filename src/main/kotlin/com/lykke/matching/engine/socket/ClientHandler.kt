package com.lykke.matching.engine.socket

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.utils.IntUtils
import org.apache.log4j.Logger
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.time.LocalDateTime
import java.util.concurrent.BlockingQueue

class ClientHandler(
        private val queue: BlockingQueue<MessageWrapper>,
        private val socket: Socket,
        private val socketServer: SocketServer): Thread() {

    companion object {
        val LOGGER = Logger.getLogger(ClientHandler::class.java.name)
    }

    @Volatile
    private var incomingSize: Long = 0
    @Volatile
    private var outgoingSize: Long = 0
    @Volatile
    private var lastMessageAt: LocalDateTime = LocalDateTime.now()

    var clientHostName = socket.inetAddress.hostAddress

    var inputStream: DataInputStream? = null
    var outputStream: DataOutputStream? = null

    override fun run() {
        LOGGER.info("Got connection from $clientHostName.")
        try {
            inputStream = DataInputStream(BufferedInputStream(socket.inputStream))
            outputStream = DataOutputStream(BufferedOutputStream(socket.outputStream))
            outputStream!!.flush()
            while (true) {
                readMessage(inputStream!!, outputStream!!)
            }
        } catch (e: Exception) {
            LOGGER.info("Connection from $clientHostName disconnected.", e)
            if (socket.isConnected && !socket.isClosed) {
                socket.close()
            }
            socketServer.disconnect(this)
        } finally {
            LOGGER.info("Connection from $clientHostName closed.")
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
        queue.put(MessageWrapper(clientHostName, type, serializedData, this))
    }

    fun writeOutput(byteArray: ByteArray) {
        if (socket.isConnected && !socket.isClosed) {
            outgoingSize += byteArray.size
            outputStream!!.write(byteArray)
            outputStream!!.flush()
        }
    }

    fun isConnected(): Boolean {
        val now = LocalDateTime.now()
        return now.minusMinutes(1).isBefore(lastMessageAt)
    }

    fun disconnect() {
        try {
            socket.close()
        } catch (e: Exception) {
            LOGGER.info("Unable to close connection to $clientHostName")
        }
    }
}