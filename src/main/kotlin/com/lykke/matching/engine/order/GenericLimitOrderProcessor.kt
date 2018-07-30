package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.context.SingleLimitContext
import com.lykke.matching.engine.daos.order.LimitOrderType
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
import java.util.*
import java.util.concurrent.BlockingQueue

class GenericLimitOrderProcessor(private val limitOrderService: GenericLimitOrderService,
                                 private val stopLimitOrderService: GenericStopLimitOrderService,
                                 private val singleLimitOrderContextParser: SingleLimitOrderContextParser,
                                 clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                 limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
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
            balancesHolder,
            limitOrderBusinessValidator,
            messageSequenceNumberHolder,
            messageSender,
            LOGGER)

    fun checkAndProcessStopOrder(messageId: String, assetPair: AssetPair, now: Date) {
        val assetPairId = assetPair.assetPairId
        val order = stopLimitOrderService.getStopOrderForProcess(assetPairId, now) ?: return
        val orderBook = limitOrderService.getOrderBook(assetPairId)
        LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} (bestBidPrice=${orderBook.getBidPrice()}, bestAskPrice=${orderBook.getAskPrice()}) due to message $messageId")
        val payBackReserved = order.reservedLimitVolume!!
        order.reservedLimitVolume = null

        val stopLimitContext = singleLimitOrderContextParser.getStopOrderContext(messageId, now, order)

        processLimitOrder(stopLimitContext, payBackReserved)
    }

    private fun processLimitOrder(messageWrapper: MessageWrapper, singleLimitContext: SingleLimitContext) {
        limitOrderProcessor.processLimitOrder(singleLimitContext, messageWrapper =  messageWrapper)
        checkAndProcessStopOrder(singleLimitContext.messageId, singleLimitContext.assetPair, singleLimitContext.orderProcessingStartTime)
    }

    fun processOrder(messageWrapper: MessageWrapper, singleLimitContext: SingleLimitContext) {
        when(singleLimitContext.limitOrder.type) {
            LimitOrderType.LIMIT -> processLimitOrder(messageWrapper, singleLimitContext)
            LimitOrderType.STOP_LIMIT -> processStopOrder(messageWrapper, singleLimitContext)
        }
    }

    fun processLimitOrder(singleLimitContext: SingleLimitContext, payBackReserved: BigDecimal) {
        limitOrderProcessor.processLimitOrder(singleLimitContext, payBackReserved)
        checkAndProcessStopOrder(singleLimitContext.messageId, singleLimitContext.assetPair, singleLimitContext.orderProcessingStartTime)
    }

    private fun processStopOrder(messageWrapper: MessageWrapper, singleLimitContext: SingleLimitContext) =
            stopLimitOrderProcessor.processStopOrder(messageWrapper, singleLimitContext)
}