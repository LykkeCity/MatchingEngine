package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LimitOrder
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

    fun checkAndProcessStopOrder(messageId: String, assetPairId: String, now: Date) {
        val order = stopLimitOrderService.getStopOrderForProcess(assetPairId, now) ?: return
        val orderBook = limitOrderService.getOrderBook(assetPairId)
        LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} (bestBidPrice=${orderBook.getBidPrice()}, bestAskPrice=${orderBook.getAskPrice()})")
        val payBackReserved = order.reservedLimitVolume!!
        order.reservedLimitVolume = null
        processLimitOrder(messageId, order, now, payBackReserved)
    }

    private fun processLimitOrder(messageId: String, messageWrapper: MessageWrapper, order: LimitOrder, isCancelOrders: Boolean, now: Date) {
        limitOrderProcessor.processLimitOrder(order, isCancelOrders, now, messageId, messageWrapper =   messageWrapper)
        checkAndProcessStopOrder(messageId, order.assetPairId, now)
    }

    fun processOrder(messageWrapper: MessageWrapper, order: LimitOrder, isCancelOrders: Boolean, now: Date) {
        when(order.type) {
            LimitOrderType.LIMIT -> processLimitOrder(messageWrapper.messageId!!, messageWrapper, order, isCancelOrders, now)
            LimitOrderType.STOP_LIMIT -> processStopOrder(messageWrapper, order, isCancelOrders, now)
        }
    }

    fun processLimitOrder(messageId: String, order: LimitOrder, now: Date, payBackReserved: BigDecimal) {
        limitOrderProcessor.processLimitOrder(order, false, now, messageId, payBackReserved)
        checkAndProcessStopOrder(messageId, order.assetPairId, now)
    }

    private fun processStopOrder(messageWrapper: MessageWrapper, order: LimitOrder, isCancelOrders: Boolean, now: Date) =
            stopLimitOrderProcessor.processStopOrder(messageWrapper, order, isCancelOrders, now)
}