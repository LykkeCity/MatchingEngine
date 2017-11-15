package com.lykke.matching.engine.outgoing.socket

import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageType.ORDER_BOOK_SNAPSHOT
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.round
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.utils.ByteHelper.Companion.toByteArray
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.util.Date
import java.util.HashSet
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.thread

class Connection(val socket: Socket,
                 val inputQueue: BlockingQueue<OrderBook>,
                 val orderBooks: ConcurrentHashMap<String, AssetOrderBook>,
                 val assetsCache: AssetsHolder,
                 val assetPairsCache: AssetsPairsHolder) : Thread() {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(Connection::class.java.name)
    }

    var connectionHolder: ConnectionsHolder? = null

    var clientHostName = socket.inetAddress.canonicalHostName
    var assetsPairs = HashSet<String>()


    override fun run() {
        LOGGER.info("Got order book subscriber from $clientHostName.")
        try {
            val inputStream = DataInputStream(BufferedInputStream(socket.inputStream))
            val outputStream = DataOutputStream(BufferedOutputStream(socket.outputStream))
            outputStream.flush()

            thread {
                while (!isClosed()) {
                    val type = inputStream.readByte()
                    if (type == MessageType.PING.type) {
                        //do not read, send back ping
                        outputStream.write(byteArrayOf(MessageType.PING.type))
                        outputStream.flush()
                    } else {
                        LOGGER.error("Unsupported message type: $type")
                    }
                }
            }

            val now = Date()
            orderBooks.values.forEach {
                val orderBook = it.copy()
                writeOrderBook(OrderBook(orderBook.assetId, true, now, orderBook.getOrderBook(true)), outputStream)
                writeOrderBook(OrderBook(orderBook.assetId, false, now, orderBook.getOrderBook(false)), outputStream)
            }

            while (true) {
                val item = inputQueue.take()
                writeOrderBook(item, outputStream)
            }
        } catch (e: Exception) {
            LOGGER.error("Order book subscriber disconnected: $clientHostName", e)
        } finally {
            LOGGER.info("Order book subscriber connection from $clientHostName closed.")
            socket.close()
            if (connectionHolder != null) {
                connectionHolder!!.removeConnection(this)
            }
        }
    }

    private fun writeOrderBook(orderBook: OrderBook, stream : DataOutputStream) {
        val builder = ProtocolMessages.OrderBookSnapshot.newBuilder().setAsset(orderBook.assetPair).setIsBuy(orderBook.isBuy).setTimestamp(orderBook.timestamp.time)
        val pair = assetPairsCache.getAssetPair(orderBook.assetPair)
        val baseAsset = assetsCache.getAsset(pair.baseAssetId)
        orderBook.prices.forEach { price ->
            builder.addLevels(ProtocolMessages.OrderBookSnapshot.OrderBookLevel.newBuilder()
                    .setPrice(price.price.round(pair.accuracy))
                    .setVolume(price.volume.round(baseAsset.accuracy)).build())
        }

        val book = builder.build()

        stream.write(toByteArray(ORDER_BOOK_SNAPSHOT.type, book.serializedSize, book.toByteArray()))
        stream.flush()
    }

    fun isClosed() : Boolean {
        return socket.isClosed
    }
}