package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.BlockingQueue

class GenericLimitOrderProcessor(private val limitOrderService: GenericLimitOrderService,
                                 private val stopLimitOrderService: GenericStopLimitOrderService,
                                 trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                                 clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                                 orderBookQueue: BlockingQueue<OrderBook>,
                                 rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                                 assetsHolder: AssetsHolder,
                                 assetsPairsHolder: AssetsPairsHolder,
                                 balancesHolder: BalancesHolder,
                                 applicationSettingsCache: ApplicationSettingsCache,
                                 lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                 matchingEngine: MatchingEngine,
                                 private val LOGGER: Logger) {

    private val limitOrderProcessor = LimitOrderProcessor(limitOrderService,
            trustedClientLimitOrderReportQueue,
            clientLimitOrderReportQueue,
            orderBookQueue,
            rabbitOrderBookQueue,
            assetsHolder,
            assetsPairsHolder,
            balancesHolder,
            applicationSettingsCache,
            lkkTradesQueue,
            matchingEngine,
            LOGGER)

    private val stopLimitOrderProcessor = StopLimitOrderProcessor(limitOrderService,
            stopLimitOrderService,
            this,
            clientLimitOrderReportQueue,
            assetsHolder,
            assetsPairsHolder,
            balancesHolder,
            applicationSettingsCache,
            LOGGER)

    fun checkAndProcessStopOrder(messageWrapper: MessageWrapper?, assetPairId: String, now: Date) {
        val order = stopLimitOrderService.getStopOrderForProcess(assetPairId) ?: return
        val orderBook = limitOrderService.getOrderBook(assetPairId)
        LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} (bestBidPrice=${orderBook.getBidPrice()}, bestAskPrice=${orderBook.getAskPrice()})")
        val payBackReserved = order.reservedLimitVolume!!
        order.reservedLimitVolume = null
        processLimitOrder(messageWrapper, order, now, payBackReserved)
    }

    private fun processLimitOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date) {
        limitOrderProcessor.processLimitOrder(messageWrapper, order, isCancelOrders, now, null)
        checkAndProcessStopOrder(messageWrapper, order.assetPairId, now)
    }

    fun processOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date) {
        when(order.type) {
            LimitOrderType.LIMIT -> processLimitOrder(messageWrapper, order, isCancelOrders, now)
            LimitOrderType.STOP_LIMIT -> processStopOrder(messageWrapper, order, isCancelOrders, now)
        }
    }

    fun processLimitOrder(messageWrapper: MessageWrapper?, order: NewLimitOrder, now: Date, payBackReserved: Double) {
        limitOrderProcessor.processLimitOrder(messageWrapper, order, false, now, payBackReserved)
        checkAndProcessStopOrder(messageWrapper, order.assetPairId, now)
    }

    private fun processStopOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date) =
            stopLimitOrderProcessor.processStopOrder(messageWrapper, order, isCancelOrders, now)
}