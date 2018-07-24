package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.context.SingleLimitContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.concurrent.BlockingQueue

class GenericLimitOrderProcessor(private val limitOrderService: GenericLimitOrderService,
                                 private val stopLimitOrderService: GenericStopLimitOrderService,
                                 private val singleLimitOrderContextParser: SingleLimitOrderContextParser,
                                 clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                 limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
                                 assetsHolder: AssetsHolder,
                                 assetsPairsHolder: AssetsPairsHolder,
                                 balancesHolder: BalancesHolder,
                                 limitOrderBusinessValidator: LimitOrderBusinessValidator,
                                 matchingEngine: MatchingEngine,
                                 messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                 messageSender: MessageSender,
                                 private val LOGGER: Logger) {

    private val limitOrderProcessor = SingleLimitOrderProcessor(limitOrderService,
            limitOrdersProcessorFactory,
            matchingEngine,
            LOGGER)

    private val stopLimitOrderProcessor = StopLimitOrderProcessor(limitOrderService,
            stopLimitOrderService,
            this,
            clientLimitOrdersQueue,
            assetsHolder,
            assetsPairsHolder,
            balancesHolder,
            limitOrderBusinessValidator,
            messageSequenceNumberHolder,
            messageSender,
            LOGGER)

    fun checkAndProcessStopOrder(singleLimitContext: SingleLimitContext) {
        val assetPairId = singleLimitContext.limitOrder.assetPairId
        val now = singleLimitContext.orderProcessingStartTime
        val order = stopLimitOrderService.getStopOrderForProcess(assetPairId, now) ?: return
        val orderBook = limitOrderService.getOrderBook(assetPairId)
        LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} (bestBidPrice=${orderBook.getBidPrice()}, bestAskPrice=${orderBook.getAskPrice()}) due to message ${singleLimitContext.messageId}")
        val payBackReserved = order.reservedLimitVolume!!
        order.reservedLimitVolume = null

        val stopLimitContext = singleLimitOrderContextParser.getContext(singleLimitContext.messageId, singleLimitContext.id,
                singleLimitContext.orderProcessingStartTime, order, singleLimitContext.isCancelOrders, singleLimitContext.processedMessage)
        processLimitOrder(stopLimitContext, payBackReserved)
    }

    private fun processLimitOrder(messageWrapper: MessageWrapper, singleLimitContext: SingleLimitContext) {
        limitOrderProcessor.processLimitOrder(singleLimitContext, messageWrapper =  messageWrapper)
        checkAndProcessStopOrder(singleLimitContext)
    }

    fun processOrder(messageWrapper: MessageWrapper, singleLimitContext: SingleLimitContext) {
        when(singleLimitContext.limitOrder.type) {
            LimitOrderType.LIMIT -> processLimitOrder(messageWrapper, singleLimitContext)
            LimitOrderType.STOP_LIMIT -> processStopOrder(messageWrapper, singleLimitContext)
        }
    }

    fun processLimitOrder(singleLimitContext: SingleLimitContext, payBackReserved: BigDecimal) {
        limitOrderProcessor.processLimitOrder(singleLimitContext, payBackReserved)
        checkAndProcessStopOrder(singleLimitContext)
    }

    private fun processStopOrder(messageWrapper: MessageWrapper, singleLimitContext: SingleLimitContext) =
            stopLimitOrderProcessor.processStopOrder(messageWrapper, singleLimitContext)
}