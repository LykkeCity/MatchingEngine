package com.lykke.matching.engine

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import org.apache.log4j.Logger
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.util.concurrent.BlockingQueue

class ClientHandler(val queue: BlockingQueue<MessageWrapper>, val socket: Socket): Thread() {

    companion object {
        val LOGGER = Logger.getLogger(ClientHandler::class.java.name)
    }

    var clientHostName = socket.inetAddress.canonicalHostName

    override fun run() {
        LOGGER.info("Got connection from $clientHostName.")
        try {
            val inputStream = DataInputStream(BufferedInputStream(socket.inputStream))
            val outputStream = DataOutputStream(BufferedOutputStream(socket.outputStream))
            outputStream.flush()
            while (true) {
                readMessage(inputStream, outputStream)
            }
        } finally {
            LOGGER.info("Connection from $clientHostName closed.")
            socket.close()
        }
    }

    private fun readMessage(inputStream: DataInputStream, outputStream: DataOutputStream) {
        val type = inputStream.readByte()
        if (type == MessageType.PING.type) {
            LOGGER.debug("Got ping request from $clientHostName.")
            //do not read, send back ping
            outputStream.write(byteArrayOf(MessageType.PING.type))
            outputStream.flush()
            return
        }

        val size = inputStream.readInt()
        val serializedData = ByteArray(size)
        inputStream.read(serializedData, 0, size)
        queue.put(MessageWrapper(MessageType.valueOf(type), serializedData, outputStream))
    }
}