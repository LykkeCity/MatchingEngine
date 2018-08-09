package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
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
                              private val cancellerFactory: GenericLimitOrdersCancellerFactory,
                              private val validator: LimitOrderCancelOperationBusinessValidator) : AbstractService {
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

        LOGGER.debug("Got limit order cancel request (id: ${context.uid}, orders: $orderIds)")
        val typeToOrder = getLimitOrderTypeToLimitOrders(orderIds)

        try {
            validator.performValidation(typeToOrder, context)
        } catch (e: ValidationException) {
            LOGGER.info(e.message)
            writeResponse(messageWrapper, MessageStatusUtils.toMessageStatus(e.validationType))
            return
        }

        val updateSuccessful = cancelOrders(typeToOrder[LimitOrderType.LIMIT], typeToOrder[LimitOrderType.STOP_LIMIT], now, context)

        if (!updateSuccessful) {
            writeResponse(messageWrapper, MessageStatus.RUNTIME, "Unable to save result")
            return
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        writeResponse(messageWrapper, status, null)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        //nothing to do
    }

    private fun cancelOrders(limitOrders: List<LimitOrder>?, stopOrders: List<LimitOrder>?, now: Date, context: LimitOrderCancelOperationContext): Boolean {
        val canceller = cancellerFactory.create(LOGGER, now)

        canceller.preProcessLimitOrders(limitOrders ?: emptyList())
        canceller.preProcessStopLimitOrders(stopOrders ?: emptyList())

        return canceller.applyFull(context.uid,
                context.messageId,
                context.processedMessage,
                context.messageType,
                false)
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

        genericLimitOrderService.cancelLimitOrder(now, context.limitOrderIds.first().toString(), true)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
    }

    private fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
            return
        }

        val builder = ProtocolMessages.NewResponse.newBuilder().setStatus(status.type)

        message?.let {
            builder.statusReason = message
        }

        messageWrapper.writeNewResponse(builder)
    }
}
