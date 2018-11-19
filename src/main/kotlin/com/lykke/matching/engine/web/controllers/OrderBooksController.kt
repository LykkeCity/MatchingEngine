package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.web.dto.StopOrder
import com.lykke.matching.engine.web.dto.StopOrderBook
import com.lykke.utils.logging.ThrottlingLogger
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.servlet.http.HttpServletRequest

@RestController
@Api(description = "Api to access order books")
class OrderBooksController {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(OrderBooksController::class.java.name)
    }

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    private lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    @Autowired
    private lateinit var midPriceHolder: MidPriceHolder

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    @GetMapping("/orderBooks", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Endpoint to get all limit order books")
    fun getOrderBooks(request: HttpServletRequest): LinkedList<OrderBook> {
        val books = LinkedList<OrderBook>()
        val now = Date()

        genericLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            val refMidPrice = midPriceHolder.getRefMidPriceWithoutCleanupAndChecks(assetsPairsHolder.getAssetPair(orderBook.assetPairId), Date())
            books.add(OrderBook(orderBook.assetPairId, refMidPrice, midPriceHolder.refreshMidPricePeriod, true, now, orderBook.getOrderBook(true)))
            books.add(OrderBook(orderBook.assetPairId, refMidPrice, midPriceHolder.refreshMidPricePeriod, false, now, orderBook.getOrderBook(false)))
        }

        LOGGER.info("Order book snapshot sent to ${request.remoteAddr}")

        return books
    }

    @GetMapping("/stopOrderBooks", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Endpoint to get all stop order books")
    fun getStopOrderBooks(request: HttpServletRequest): List<StopOrderBook> {
        val books = LinkedList<StopOrderBook>()
        val now = Date()

        genericStopLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            books.add(StopOrderBook(orderBook.assetPairId, true, true, now, toStopOrderDto(orderBook.getOrderBook(true, true))))
            books.add(StopOrderBook(orderBook.assetPairId, true, false, now, toStopOrderDto(orderBook.getOrderBook(true, false))))
            books.add(StopOrderBook(orderBook.assetPairId, false, true, now, toStopOrderDto(orderBook.getOrderBook(false, true))))
            books.add(StopOrderBook(orderBook.assetPairId, false, false, now, toStopOrderDto(orderBook.getOrderBook(false, false))))
        }

        LOGGER.info("Stop order book snapshot sent to ${request.remoteAddr}")

        return books
    }

    private fun toStopOrderDto(limitOrders: Collection<LimitOrder>): List<StopOrder> {
        return limitOrders.map { limitOrder ->
            StopOrder(limitOrder.externalId,
                    limitOrder.clientId,
                    limitOrder.volume,
                    limitOrder.lowerLimitPrice,
                    limitOrder.lowerPrice,
                    limitOrder.upperLimitPrice,
                    limitOrder.upperPrice)
        }
    }

    @ExceptionHandler(Exception::class)
    private fun handleException(request: HttpServletRequest, ex: Exception): ResponseEntity<*> {
        LOGGER.error("Unable to write order book snapshot request to ${request.remoteAddr}", ex)
        return ResponseEntity<Any>(null, HttpStatus.INTERNAL_SERVER_ERROR)
    }
}