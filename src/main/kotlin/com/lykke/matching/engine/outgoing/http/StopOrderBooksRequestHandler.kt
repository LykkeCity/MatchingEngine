package com.lykke.matching.engine.outgoing.http

import com.google.gson.GsonBuilder
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.utils.logging.ThrottlingLogger
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

class StopOrderBooksRequestHandler(val genericStopLimitOrderService: GenericStopLimitOrderService) : HttpHandler {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(StopOrderBooksRequestHandler::class.java.name)
    }

    override fun handle(exchange: HttpExchange) {
        try {
            val books = LinkedList<StopOrderBook>()
            val now = Date()
            genericStopLimitOrderService.getAllOrderBooks().values.forEach {
                val orderBook = it.copy()
                books.add(StopOrderBook(orderBook.assetPairId, true, true, now, orderBook.getOrderBook(true, true)))
                books.add(StopOrderBook(orderBook.assetPairId, true, false, now, orderBook.getOrderBook(true, false)))
                books.add(StopOrderBook(orderBook.assetPairId, false, true, now, orderBook.getOrderBook(false, true)))
                books.add(StopOrderBook(orderBook.assetPairId, false, false, now, orderBook.getOrderBook(false, false)))
            }

            val response = GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create().toJson(books)
            exchange.sendResponseHeaders(200, response.length.toLong())
            val os = exchange.responseBody
            os.write(response.toByteArray())
            os.close()
            LOGGER.info("Stop order book snapshot sent to ${exchange.remoteAddress}")
        } catch (e: Exception) {
            LOGGER.error("Unable to write stop order book snapshot request to ${exchange.remoteAddress}", e)
        }
    }
}

private class StopOrderBook(val assetPair: String,
                            val isBuy: Boolean,
                            val isLower: Boolean,
                            val timestamp: Date,
                            orders: PriorityBlockingQueue<LimitOrder>) : JsonSerializable() {

    val prices: MutableList<Order> = ArrayList()

    init {
        while (!orders.isEmpty()) {
            val order = orders.poll()
            prices.add(Order(order.externalId,
                    order.clientId,
                    order.volume,
                    order.lowerLimitPrice,
                    order.lowerPrice,
                    order.upperLimitPrice,
                    order.upperPrice))
        }
    }
}

private class Order(val id: String,
                    val clientId: String,
                    val volume: BigDecimal,
                    val lowerLimitPrice: BigDecimal?,
                    val lowerPrice: BigDecimal?,
                    val upperLimitPrice: BigDecimal?,
                    val upperPrice: BigDecimal?)