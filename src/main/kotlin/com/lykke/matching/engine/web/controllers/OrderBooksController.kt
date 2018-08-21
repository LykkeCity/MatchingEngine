package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.outgoing.http.StopOrderBook
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.servlet.http.HttpServletRequest

@RestController
class OrderBooksController {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(OrderBooksController::class.java.name)
    }

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    private lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    @GetMapping("/orderBooks")
    fun getOrderBooks(request: HttpServletRequest): LinkedList<OrderBook> {
        val books = LinkedList<OrderBook>()
        val now = Date()

        genericLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            books.add(OrderBook(orderBook.assetId, true, now, orderBook.getOrderBook(true)))
            books.add(OrderBook(orderBook.assetId, false, now, orderBook.getOrderBook(false)))
        }

        LOGGER.info("Order book snapshot sent to ${request.remoteAddr}")

        return books
    }

    @GetMapping("/stopOrderBooks")
    fun getStopOrderBooks() {
        val books = LinkedList<StopOrderBook>()
        val now = Date()
        genericStopLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            books.add(StopOrderBook(orderBook.assetPairId, true, true, now, orderBook.getOrderBook(true, true)))
            books.add(StopOrderBook(orderBook.assetPairId, true, false, now, orderBook.getOrderBook(true, false)))
            books.add(StopOrderBook(orderBook.assetPairId, false, true, now, orderBook.getOrderBook(false, true)))
            books.add(StopOrderBook(orderBook.assetPairId, false, false, now, orderBook.getOrderBook(false, false)))
        }

    }
}