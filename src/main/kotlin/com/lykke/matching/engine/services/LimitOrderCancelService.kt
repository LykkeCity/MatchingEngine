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
                              private val validator: LimitOrderCancelOperationBusinessValidator,
                              private val persistenceManager: PersistenceManager) : AbstractService {
    companion object {

        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }
    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val context = messageWrapper.context as LimitOrderCancelOperationContext

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            processOldLimitOrderCancelMessage(messageWrapper, now)
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

        val updateSuccessful = cancelOrders(typeToOrder[LimitOrderType.LIMIT], typeToOrder[LimitOrderType.STOP_LIMIT], now, messageWrapper)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updateSuccessful

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

    private fun cancelOrders(limitOrders: List<LimitOrder>?, stopOrders: List<LimitOrder>?, now: Date, messageWrapper: MessageWrapper): Boolean {
        val canceller = cancellerFactory.create(LOGGER, now)

        canceller.preProcessLimitOrders(limitOrders ?: emptyList())
        canceller.preProcessStopLimitOrders(stopOrders ?: emptyList())

        return canceller.applyFull(messageWrapper.id!!,
                messageWrapper.messageId!!,
                messageWrapper.processedMessage(),
                MessageType.valueOf(messageWrapper.type) ?: throw Exception("Unknown message type ${messageWrapper.type}"),
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

    private fun processOldLimitOrderCancelMessage(messageWrapper: MessageWrapper, now: Date) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldLimitOrderCancel
        LOGGER.debug("Got old limit  order messageId: ${messageWrapper.messageId}  (id: ${message.limitOrderId}) cancel request id: ${message.uid}")
        val order = genericLimitOrderService.getOrder(message.limitOrderId.toString())
        if (order != null) {
            val newOrderBook = genericLimitOrderService.getOrderBook(order.assetPairId).copy()
            newOrderBook.removeOrder(order)
            val updated = persistenceManager.persist(PersistenceData(null,
                    messageWrapper.processedMessage(),
                    OrderBooksPersistenceData(listOf(OrderBookPersistenceData(order.assetPairId,
                            order.isBuySide(),
                            newOrderBook.getCopyOfOrderBook(order.isBuySide()))),
                            emptyList(),
                            listOf(order)),
                    null,
                    null))
            if (updated) {
                genericLimitOrderService.cancelLimitOrder(Date(), message.limitOrderId.toString(), true)
            }
        }
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
