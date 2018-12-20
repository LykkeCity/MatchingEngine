package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.process.common.CancelRequest
import com.lykke.matching.engine.performance.PerformanceStatsHolder
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
                              private val limitOrdersCancelServiceHelper: LimitOrdersCancelServiceHelper,
                              private val performanceStatsHolder: PerformanceStatsHolder) : AbstractService {
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

        limitOrdersCancelServiceHelper.cancelOrdersAndWriteResponse(CancelRequest(ordersByType[LimitOrderType.LIMIT] ?: emptyList(),
                ordersByType[LimitOrderType.STOP_LIMIT] ?: emptyList(),
                null,
                context.messageId,
                context.uid,
                context.messageType,
                now,
                context.processedMessage,
                messageWrapper,
                LOGGER))
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val start = System.nanoTime()
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
        val end = System.nanoTime()
        performanceStatsHolder.addWriteResponseTime(MessageType.LIMIT_ORDER_CANCEL.type, end - start)
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
}
