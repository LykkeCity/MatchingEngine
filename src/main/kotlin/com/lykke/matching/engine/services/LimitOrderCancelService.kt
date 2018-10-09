package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.business.LimitOrderCancelOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*
import java.util.stream.Collectors

@Service
class LimitOrderCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                              private val genericStopLimitOrderService: GenericStopLimitOrderService,
                              private val validator: LimitOrderCancelOperationBusinessValidator,
                              private val persistenceManager: PersistenceManager,
                              private val limitOrdersCancelHelper: LimitOrdersCancelHelper) : AbstractService {


    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val context = messageWrapper.context as LimitOrderCancelOperationContext

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            processOldLimitOrderCancelMessage(messageWrapper, context, now)
        }

        val orderIds = context.limitOrderIds
        LOGGER.debug("Got limit order cancel request (messageId: ${messageWrapper.messageId}, id: ${context.uid}, orders: $orderIds)")
        val typeToOrder = getLimitOrderTypeToLimitOrders(orderIds)

        try {
            validator.performValidation(typeToOrder, context)
        } catch (e: ValidationException) {
            LOGGER.info("Business validation failed: ${context.messageId}, details: ${e.message}")
            writeResponse(messageWrapper, MessageStatusUtils.toMessageStatus(e.validationType))
            return
        }

        val updateSuccessful = limitOrdersCancelHelper.cancelOrders(LimitOrdersCancelHelper.CancelRequest(context.uid,
                context.messageId,
                context.messageType,
                typeToOrder[LimitOrderType.LIMIT],
                typeToOrder[LimitOrderType.STOP_LIMIT], now, context.processedMessage, false))


        limitOrdersCancelHelper.processPersistResults(updateSuccessful, messageWrapper, context.messageId)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        //nothing to do
    }

    private fun getLimitOrderTypeToLimitOrders(orderIds: Set<String>): Map<LimitOrderType, List<LimitOrder>> {
        return orderIds.stream()
                .map(::getOrder)
                .filter { limitOrder -> limitOrder != null }
                .map { t -> t!! }
                .collect(Collectors.groupingBy({ limitOrder: LimitOrder -> limitOrder.type!! }))
    }

    private fun getOrder(orderId: String): LimitOrder? {
        return genericLimitOrderService.getOrder(orderId) ?: genericStopLimitOrderService.getOrder(orderId)
    }

    private fun processOldLimitOrderCancelMessage(messageWrapper: MessageWrapper, context: LimitOrderCancelOperationContext,  now: Date) {
        LOGGER.debug("Got old limit  order messageId: ${context.messageId}  (id: ${context.limitOrderIds}) cancel request id: ${context.uid}")
        val order = genericLimitOrderService.getOrder(context.limitOrderIds.first().toString())
        if (order != null) {
            val newOrderBook = genericLimitOrderService.getOrderBook(order.assetPairId).copy()
            newOrderBook.removeOrder(order)
            val updated = persistenceManager.persist(PersistenceData(null,
                    context.processedMessage,
                    OrderBooksPersistenceData(listOf(OrderBookPersistenceData(order.assetPairId,
                            order.isBuySide(),
                            newOrderBook.getCopyOfOrderBook(order.isBuySide()))),
                            emptyList(),
                            listOf(order)),
                    null,
                    null))
            if (updated) {
                genericLimitOrderService.cancelLimitOrder(Date(), context.limitOrderIds.first(), true)
            }
        }
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
    }
}
