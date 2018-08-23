package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.web.dto.StopOrderDto
import com.lykke.matching.engine.web.dto.StopOrderBookDto
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
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

    @GetMapping("/orderBooks", produces = [MediaType.APPLICATION_JSON_VALUE])
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

    @GetMapping("/stopOrderBooks", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getStopOrderBooks(request: HttpServletRequest): List<StopOrderBookDto> {
        val books = LinkedList<StopOrderBookDto>()
        val now = Date()

        genericStopLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            books.add(StopOrderBookDto(orderBook.assetPairId, true, true, now, toStopOrderDto(orderBook.getOrderBook(true, true))))
            books.add(StopOrderBookDto(orderBook.assetPairId, true, false, now, toStopOrderDto(orderBook.getOrderBook(true, false))))
            books.add(StopOrderBookDto(orderBook.assetPairId, false, true, now, toStopOrderDto(orderBook.getOrderBook(false, true))))
            books.add(StopOrderBookDto(orderBook.assetPairId, false, false, now, toStopOrderDto(orderBook.getOrderBook(false, false))))
        }

        LOGGER.info("Stop order book snapshot sent to ${request.remoteAddr}")
        return books
    }

    private fun toStopOrderDto(limitOrders: Collection<LimitOrder>): List<StopOrderDto> {
        return limitOrders.map { limitOrder ->
            StopOrderDto(limitOrder.externalId,
                    limitOrder.clientId,
                    limitOrder.volume,
                    limitOrder.lowerLimitPrice,
                    limitOrder.lowerPrice,
                    limitOrder.upperLimitPrice,
                    limitOrder.upperPrice)
        }
    }
}