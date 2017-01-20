package com.lykke.matching.engine.outgoing.socket

import com.lykke.matching.engine.outgoing.JsonSerializable
import com.lykke.matching.engine.outgoing.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import org.apache.log4j.Logger
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.DataOutputStream
import java.io.InputStreamReader
import java.net.Socket
import java.net.SocketException
import java.util.Date
import java.util.HashSet
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.thread

class Connection(val socket: Socket, val inputQueue: BlockingQueue<JsonSerializable>, val orderBooks: ConcurrentHashMap<String, AssetOrderBook>) : Thread() {

    companion object {
        val LOGGER = Logger.getLogger(Connection::class.java.name)
        val CR_DELIMITER : Byte = '\r'.toByte()
        val LF_DELIMITER : Byte = '\n'.toByte()
        val PING = "ping"
    }

    var connectionHolder: ConnectionsHolder? = null

    var clientHostName = socket.inetAddress.canonicalHostName
    var assetsPairs = HashSet<String>()


    override fun run() {
        LOGGER.info("Got order book subscriber from $clientHostName.")
        try {
            val outputStream = DataOutputStream(BufferedOutputStream(socket.outputStream))
            val inputStream = BufferedReader(InputStreamReader(socket.inputStream))
            outputStream.flush()

            thread {
                while (!isClosed()) {
                    val input = inputStream.readLine()
                    if (PING == input) {
                        outputStream.write(toByteArray(PING.toByteArray()))
                        outputStream.flush()
                    } else if (input != null) {
                        LOGGER.error("Unknown input message: $input")
                    }
                }
            }

            val now = Date()
            orderBooks.values.forEach {
                val orderBook = it.copy()
                writeOrderBook(OrderBook(orderBook.assetId, true, now, orderBook.bidOrderBook), outputStream)
                writeOrderBook(OrderBook(orderBook.assetId, false, now, orderBook.askOrderBook), outputStream)
            }

            while (true) {
                val item = inputQueue.take()
                writeOrderBook(item, outputStream)
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

    private fun writeOrderBook(item: JsonSerializable, stream : DataOutputStream) {
        stream.write(toByteArray(item.toJson().toByteArray()))
        stream.flush()
    }

    fun toByteArray(data: ByteArray): ByteArray {
        val result = ByteArray(data.size + 2)

        System.arraycopy(data, 0, result, 0, data.size)
        result[data.size] = CR_DELIMITER
        result[data.size+1] = LF_DELIMITER

        return result
    }

    fun isClosed() : Boolean {
        return socket.isClosed
    }
}