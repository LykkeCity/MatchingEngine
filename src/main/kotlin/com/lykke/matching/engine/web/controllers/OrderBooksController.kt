package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*

@RestController
class OrderBooksController {

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @GetMapping("/orderBooks")
    fun getOrderBooks(): LinkedList<OrderBook> {
        val books = LinkedList<OrderBook>()
        val now = Date()

        genericLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            books.add(OrderBook(orderBook.assetId, true, now, orderBook.getOrderBook(true)))
            books.add(OrderBook(orderBook.assetId, false, now, orderBook.getOrderBook(false)))
        }

        return books
    }

    @GetMapping("/stopOrderBooks")
    fun getStopOrderBooks() {

    }
}