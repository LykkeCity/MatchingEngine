package com.lykke.matching.engine.outgoing

import org.apache.log4j.Logger
import java.io.BufferedOutputStream
import java.io.DataOutputStream
import java.net.Socket
import java.net.SocketException
import java.util.HashSet
import java.util.concurrent.BlockingQueue

class Connection(val socket: Socket, val inputQueue: BlockingQueue<OrderBook>) : Thread() {

    companion object {
        val LOGGER = Logger.getLogger(Connection::class.java.name)
    }

    var connectionHolder: ConnectionsHolder? = null

    var clientHostName = socket.inetAddress.canonicalHostName
    var assetsPairs = HashSet<String>()


    override fun run() {
        LOGGER.info("Got order book subscriber from $clientHostName.")
        try {
            val outputStream = DataOutputStream(BufferedOutputStream(socket.outputStream))
            outputStream.flush()

            while (true) {
                val orderBook = inputQueue.take()
                writePrice(orderBook, outputStream)
            }
        } catch (e: SocketException) {
            LOGGER.error("Order book subscriber disconnected: $clientHostName")
        } finally {
            LOGGER.info("Order book subscriber connection from $clientHostName closed.")
            socket.close()
            if (connectionHolder != null) {
                connectionHolder!!.removeConnection(this)
            }
        }
    }

    private fun writePrice(orderBook: OrderBook, stream : DataOutputStream) {
        stream.write(toByteArray(orderBook.toJson().toByteArray()))
        stream.flush()
    }

    fun toByteArray(data: ByteArray): ByteArray {
        val result = ByteArray(4 + data.size)
        //convert to little endian
        result[0] = data.size.toByte()
        result[1] = data.size.ushr(8).toByte()
        result[2] = data.size.ushr(16).toByte()
        result[3] = data.size.ushr(24).toByte()

        System.arraycopy(data, 0, result, 4, data.size)

        return result
    }

    fun isClosed() : Boolean {
        return socket.isClosed
    }
}