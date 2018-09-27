package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.BlockingQueue

class StopLimitOrderProcessor(private val limitOrderService: GenericLimitOrderService,
                              private val stopLimitOrderService: GenericStopLimitOrderService,
                              private val genericLimitOrderProcessor: GenericLimitOrderProcessor,
                              private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                              private val balancesHolder: BalancesHolder,
                              private val stopOrderBusinessValidator: StopOrderBusinessValidator,
                              private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                              private val messageSender: MessageSender,
                              private val LOGGER: Logger) {

    fun processStopOrder(messageWrapper: MessageWrapper, singleLimitContext: SingleLimitOrderContext, now: Date) {
        val order = singleLimitContext.limitOrder

        val limitAsset = singleLimitContext.limitAsset
        val limitVolume = if (order.isBuySide()) {
            val limitPrice = order.upperPrice ?: order.lowerPrice
            if (limitPrice != null)
                NumberUtils.setScaleRoundUp(order.volume * limitPrice, limitAsset!!.accuracy)
            else null
        } else order.getAbsVolume()

        val balance = balancesHolder.getBalance(order.clientId, limitAsset!!.assetId)
        val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset.assetId)
        val clientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
        var cancelVolume = BigDecimal.ZERO
        val ordersToCancel = mutableListOf<LimitOrder>()
        if (singleLimitContext.isCancelOrders) {
            stopLimitOrderService.searchOrders(order.clientId, order.assetPairId, order.isBuySide()).forEach { orderToCancel ->
                ordersToCancel.add(orderToCancel)
                clientLimitOrdersReport.orders.add(LimitOrderWithTrades(orderToCancel))
                cancelVolume += orderToCancel.reservedLimitVolume!!
            }
        }

        val availableBalance = NumberUtils.setScaleRoundHalfUp(balancesHolder.getAvailableBalance(order.clientId, limitAsset.assetId, cancelVolume), limitAsset.accuracy)

        val orderValidationResult = validateOrder(availableBalance, limitVolume, singleLimitContext)

        if (!orderValidationResult.isValid) {
            processInvalidOrder(messageWrapper, singleLimitContext,
                    orderValidationResult, balance, reservedBalance,
                    cancelVolume, ordersToCancel, clientLimitOrdersReport, now)
            return
        }

        val orderBook = limitOrderService.getOrderBook(order.assetPairId)
        val bestBidPrice = orderBook.getBidPrice()
        val bestAskPrice = orderBook.getAskPrice()

        var price: BigDecimal? = null
        if (order.lowerLimitPrice != null && (order.isBuySide() && bestAskPrice > BigDecimal.ZERO && bestAskPrice <= order.lowerLimitPrice ||
                !order.isBuySide() && bestBidPrice > BigDecimal.ZERO && bestBidPrice <= order.lowerLimitPrice)) {
            price = order.lowerPrice
        } else if (order.upperLimitPrice != null && (order.isBuySide() && bestAskPrice >= order.upperLimitPrice ||
                !order.isBuySide() && bestBidPrice >= order.upperLimitPrice)) {
            price = order.upperPrice
        }

        if (price != null) {
            LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} immediately (bestBidPrice=$bestBidPrice, bestAskPrice=$bestAskPrice)")
            order.updateStatus(OrderStatus.InOrderBook, now)
            order.price = price

            genericLimitOrderProcessor.processLimitOrder(singleLimitContext, now, BigDecimal.ZERO)
            writeResponse(messageWrapper, order, MessageStatus.OK)
            return
        }

        val newReservedBalance = NumberUtils.setScaleRoundHalfUp(reservedBalance - cancelVolume + limitVolume!!, limitAsset.accuracy)

        val clientBalanceUpdates = listOf(ClientBalanceUpdate(order.clientId,
                limitAsset.assetId,
                balance,
                balance,
                reservedBalance,
                newReservedBalance))

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()

        val updated = balancesHolder.updateReservedBalance(singleLimitContext.processedMessage,
                sequenceNumber,
                order.clientId,
                limitAsset.assetId,
                newReservedBalance)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated

        if (!updated) {
            writePersistenceErrorResponse(messageWrapper, order)
            return
        }

        balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId,
                MessageType.LIMIT_ORDER.name,
                now,
                clientBalanceUpdates,
                messageWrapper.messageId!!))
        stopLimitOrderService.cancelStopLimitOrders(order.assetPairId, order.isBuySide(), ordersToCancel, now)

        order.reservedLimitVolume = limitVolume
        stopLimitOrderService.addStopOrder(order)

        clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))

        writeResponse(messageWrapper, order, MessageStatus.OK)
        LOGGER.info("${orderInfo(order)} added to stop order book")

        clientLimitOrdersQueue.put(clientLimitOrdersReport)

        val outgoingMessage = EventFactory.createExecutionEvent(sequenceNumber,
                messageWrapper.messageId!!,
                messageWrapper.id!!,
                now,
                MessageType.LIMIT_ORDER,
                clientBalanceUpdates,
                clientLimitOrdersReport.orders)
        messageSender.sendMessage(outgoingMessage)
    }

    private fun processInvalidOrder(messageWrapper: MessageWrapper, singleLimitContext: SingleLimitOrderContext,
                                    orderValidationResult: OrderValidationResult,
                                    balance: BigDecimal, reservedBalance: BigDecimal,
                                    cancelVolume: BigDecimal,
                                    ordersToCancel: List<LimitOrder>,
                                    clientLimitOrdersReport: LimitOrdersReport,
                                    now: Date) {
        val limitOrder = singleLimitContext.limitOrder
        val limitAsset = singleLimitContext.limitAsset

        LOGGER.info("Rejected order: ${orderInfo(limitOrder)}, rejection reason: ${orderValidationResult.message}")
        limitOrder.updateStatus(orderValidationResult.status!!, now)
        val messageStatus = MessageStatusUtils.toMessageStatus(orderValidationResult.status)
        var updated = true
        val clientBalanceUpdates = mutableListOf<ClientBalanceUpdate>()

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        if (cancelVolume > BigDecimal.ZERO) {
            val newReservedBalance = NumberUtils.setScaleRoundHalfUp(reservedBalance - cancelVolume, limitAsset!!.accuracy)
            updated = balancesHolder.updateReservedBalance(singleLimitContext.processedMessage,
                    sequenceNumber,
                    limitOrder.clientId,
                    limitAsset.assetId,
                    newReservedBalance)
            messageWrapper.triedToPersist = true
            messageWrapper.persisted = updated
            if (updated) {
                clientBalanceUpdates.add(ClientBalanceUpdate(limitOrder.clientId, limitAsset.assetId, balance, balance, reservedBalance, newReservedBalance))
                balancesHolder.sendBalanceUpdate(BalanceUpdate(limitOrder.externalId, MessageType.LIMIT_ORDER.name, Date(),
                        clientBalanceUpdates,
                        messageWrapper.messageId!!))
            }
        }

        if (updated) {
            stopLimitOrderService.cancelStopLimitOrders(limitOrder.assetPairId, limitOrder.isBuySide(), ordersToCancel, now)
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setId(limitOrder.externalId)
                    .setMatchingEngineId(limitOrder.id)
                    .setMessageId(messageWrapper.messageId)
                    .setStatus(messageStatus.type))

            clientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
            clientLimitOrdersQueue.put(clientLimitOrdersReport)

            val outgoingMessage = EventFactory.createExecutionEvent(sequenceNumber,
                    messageWrapper.messageId!!,
                    messageWrapper.id!!,
                    now,
                    MessageType.LIMIT_ORDER,
                    clientBalanceUpdates,
                    clientLimitOrdersReport.orders)
            messageSender.sendMessage(outgoingMessage)
        } else {
            writePersistenceErrorResponse(messageWrapper, limitOrder)
        }
        return
    }

    private fun validateOrder(availableBalance: BigDecimal, limitVolume: BigDecimal?, singleLimitContext: SingleLimitOrderContext): OrderValidationResult {
        if (!singleLimitContext.validationResult!!.isValid) {
            return singleLimitContext.validationResult!!
        }

        if (limitVolume != null) {
            try {
                stopOrderBusinessValidator.performValidation(availableBalance, limitVolume)
            } catch (e: OrderValidationException) {
                return OrderValidationResult(false, false, e.message, e.orderStatus)
            }
        }

        return OrderValidationResult(true)
    }

    private fun orderInfo(order: LimitOrder): String {
        return "Stop limit order (id: ${order.externalId})"
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: LimitOrder, status: MessageStatus, reason: String? = null) {
        val builder = ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(order.id)
                .setStatus(status.type)
        if (reason != null) {
            builder.statusReason = reason
        }
        messageWrapper.writeNewResponse(builder)
    }

    private fun writePersistenceErrorResponse(messageWrapper: MessageWrapper, order: LimitOrder) {
        val message = "Unable to save result data"
        LOGGER.error("$message (stop limit order id ${order.externalId})")
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(order.id)
                .setStatus(MessageStatusUtils.toMessageStatus(order.status).type)
                .setStatusReason(message))
    }
}