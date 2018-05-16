package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.concurrent.BlockingQueue

class GenericLimitOrderProcessor(private val limitOrderService: GenericLimitOrderService,
                                 private val stopLimitOrderService: GenericStopLimitOrderService,
                                 limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
                                 clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                                 assetsHolder: AssetsHolder,
                                 assetsPairsHolder: AssetsPairsHolder,
                                 balancesHolder: BalancesHolder,
                                 applicationSettingsCache: ApplicationSettingsCache,
                                 matchingEngine: MatchingEngine,
                                 private val LOGGER: Logger) {

    private val limitOrderProcessor = SingleLimitOrderProcessor(limitOrderService,
            limitOrdersProcessorFactory,
            assetsPairsHolder,
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

    fun checkAndProcessStopOrder(assetPairId: String, now: Date) {
        val order = stopLimitOrderService.getStopOrderForProcess(assetPairId) ?: return
        val orderBook = limitOrderService.getOrderBook(assetPairId)
        LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} (bestBidPrice=${orderBook.getBidPrice()}, bestAskPrice=${orderBook.getAskPrice()})")
        val payBackReserved = order.reservedLimitVolume!!
        order.reservedLimitVolume = null
        processLimitOrder(order, now, payBackReserved)
    }

    private fun processLimitOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date) {
        limitOrderProcessor.processLimitOrder(order, isCancelOrders, now, messageWrapper = messageWrapper)
        checkAndProcessStopOrder(order.assetPairId, now)
    }

    fun processOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date) {
        when(order.type) {
            LimitOrderType.LIMIT -> processLimitOrder(messageWrapper, order, isCancelOrders, now)
            LimitOrderType.STOP_LIMIT -> processStopOrder(messageWrapper, order, isCancelOrders, now)
        }
    }

    fun processLimitOrder(order: NewLimitOrder, now: Date, payBackReserved: BigDecimal) {
        limitOrderProcessor.processLimitOrder(order, false, now, payBackReserved)
        checkAndProcessStopOrder(order.assetPairId, now)
    }

    private fun processStopOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date) =
            stopLimitOrderProcessor.processStopOrder(messageWrapper, order, isCancelOrders, now)
}