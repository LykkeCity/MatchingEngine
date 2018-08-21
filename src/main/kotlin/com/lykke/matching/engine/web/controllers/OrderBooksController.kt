package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.web.dto.OrderDto
import com.lykke.matching.engine.web.dto.StopOrderBookDto
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
    fun getStopOrderBooks(request: HttpServletRequest): List<StopOrderBookDto> {
        val books = LinkedList<StopOrderBookDto>()
        val now = Date()

        genericStopLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            books.add(StopOrderBookDto(orderBook.assetPairId, true, true, now, toOrderDto(orderBook.getOrderBook(true, true))))
            books.add(StopOrderBookDto(orderBook.assetPairId, true, false, now, toOrderDto(orderBook.getOrderBook(true, false))))
            books.add(StopOrderBookDto(orderBook.assetPairId, false, true, now, toOrderDto(orderBook.getOrderBook(false, true))))
            books.add(StopOrderBookDto(orderBook.assetPairId, false, false, now, toOrderDto(orderBook.getOrderBook(false, false))))
        }

        LOGGER.info("Stop order book snapshot sent to ${request.remoteAddr}")
        return books
    }

    private fun toOrderDto(limitOrders: Collection<LimitOrder>): List<OrderDto> {
        return limitOrders.map { limitOrder ->
            OrderDto(limitOrder.externalId,
                    limitOrder.clientId,
                    limitOrder.volume,
                    limitOrder.lowerLimitPrice,
                    limitOrder.lowerPrice,
                    limitOrder.upperLimitPrice,
                    limitOrder.upperPrice)
        }
    }
}