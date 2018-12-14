package com.lykke.matching.engine.outgoing.socket

import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageType.ORDER_BOOK_SNAPSHOT
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.utils.ByteHelper.Companion.toByteArray
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.ThrottlingLogger
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.thread

class Connection(val socket: Socket,
                 val inputQueue: BlockingQueue<OrderBook>,
                 val orderBooks: ConcurrentHashMap<String, AssetOrderBook>,
                 val assetsCache: AssetsHolder,
                 val assetPairsCache: AssetsPairsHolder) : Thread(Connection::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(Connection::class.java.name)
    }

    var connectionHolder: ConnectionsHolder? = null

    var clientHostName = socket.inetAddress.canonicalHostName

    override fun run() {
        Thread.currentThread().name = "orderbook-subscriber-connection-$clientHostName"
        LOGGER.info("Got order book subscriber from $clientHostName.")
        try {
            val inputStream = DataInputStream(BufferedInputStream(socket.inputStream))
            val outputStream = DataOutputStream(BufferedOutputStream(socket.outputStream))
            outputStream.flush()

            thread(name = "${Connection::class.java.name}.inputStreamListener") {
                try {
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
                } catch (e: Exception) { }
            }

            val now = Date()
            orderBooks.values.forEach {
                val orderBook = it.copy()
                writeOrderBook(OrderBook(orderBook.assetPairId, true, now, orderBook.getOrderBook(true)), outputStream)
                writeOrderBook(OrderBook(orderBook.assetPairId, false, now, orderBook.getOrderBook(false)), outputStream)
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

    private fun writeOrderBook(orderBook: OrderBook, stream: DataOutputStream) {
        val builder = ProtocolMessages.OrderBookSnapshot.newBuilder().setAsset(orderBook.assetPair).setIsBuy(orderBook.isBuy).setTimestamp(orderBook.timestamp.time)
        val pair = assetPairsCache.getAssetPair(orderBook.assetPair)
        val baseAsset = assetsCache.getAsset(pair.baseAssetId)
        orderBook.prices.forEach { orderBookPrice ->
            builder.addLevels(ProtocolMessages.OrderBookSnapshot.OrderBookLevel.newBuilder()
                    .setPrice(NumberUtils.setScaleRoundHalfUp(orderBookPrice.price, pair.accuracy).toPlainString())
                    .setVolume(NumberUtils.setScaleRoundHalfUp(orderBookPrice.volume, baseAsset.accuracy).toPlainString()).build())
        }

        val book = builder.build()

        stream.write(toByteArray(ORDER_BOOK_SNAPSHOT.type, book.serializedSize, book.toByteArray()))
        stream.flush()
    }

    fun isClosed(): Boolean {
        return socket.isClosed
    }

    override fun toString(): String {
        return "Connection, (clientHostName: $clientHostName)"
    }
}