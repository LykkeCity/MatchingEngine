package com.lykke.matching.engine.outgoing.http

import com.google.gson.GsonBuilder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.utils.logging.ThrottlingLogger
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.util.Date
import java.util.LinkedList

class OrderBooksRequestHandler(val genericLimitOrderService: GenericLimitOrderService) : HttpHandler {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(OrderBooksRequestHandler::class.java.name)
    }

    override fun handle(exchange: HttpExchange) {
        try {
            val books = LinkedList<OrderBook>()
            val now = Date()
            genericLimitOrderService.getAllOrderBooks().values.forEach {
                val orderBook = it.copy()
                books.add(OrderBook(orderBook.assetId, true, now, orderBook.getOrderBook(true)))
                books.add(OrderBook(orderBook.assetId, false, now, orderBook.getOrderBook(false)))
            }

            val response = GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create().toJson(books)
            exchange.sendResponseHeaders(200, response.length.toLong())
            val os = exchange.responseBody
            os.write(response.toByteArray())
            os.close()
            LOGGER.info("Order book snapshot sent to ${exchange.remoteAddress}")
        } catch (e: Exception) {
            LOGGER.error("Unable to write order book snapshot request to ${exchange.remoteAddress}", e)
        }
    }
}