package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
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
                              private val limitOrdersCancelHelper: LimitOrdersCancelHelper,
                              private val messageProcessingStatusHolder: MessageProcessingStatusHolder) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val context = messageWrapper.context as LimitOrderCancelOperationContext


        LOGGER.debug("Got limit order cancel request (id: ${context.uid}, orders: ${context.limitOrderIds})")
        val ordersByType = getLimitOrderTypeToLimitOrders(context.limitOrderIds)

        try {
            validator.performValidation(ordersByType, context)
        } catch (e: ValidationException) {
            LOGGER.info("Business validation failed: ${context.messageId}, details: ${e.message}")
            writeResponse(messageWrapper, MessageStatusUtils.toMessageStatus(e.validationType))
            return
        }

        if (isMessageProcessingDisabled(ordersByType)) {
            writeResponse(messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return
        }

        val updateSuccessful = limitOrdersCancelHelper.cancelOrders(LimitOrdersCancelHelper.CancelRequest(context.uid,
                context.messageId,
                context.messageType,
                ordersByType[LimitOrderType.LIMIT],
                ordersByType[LimitOrderType.STOP_LIMIT], now, context.processedMessage, false,
                messageWrapper))


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
                .collect(Collectors.groupingBy { limitOrder: LimitOrder -> limitOrder.type!! })
    }

    private fun getOrder(orderId: String): LimitOrder? {
        return genericLimitOrderService.getOrder(orderId) ?: genericStopLimitOrderService.getOrder(orderId)
    }

    private fun isMessageProcessingDisabled(ordersByType: Map<LimitOrderType, List<LimitOrder>>): Boolean {
        return ordersByType
                .values
                .flatMap { it }
                .map {it.assetPairId}
                .toSet()
                .any {
                    !messageProcessingStatusHolder.isMessageProcessingEnabled(DisabledFunctionalityRule(null, it, MessageType.LIMIT_ORDER_CANCEL))
                }
    }
}
