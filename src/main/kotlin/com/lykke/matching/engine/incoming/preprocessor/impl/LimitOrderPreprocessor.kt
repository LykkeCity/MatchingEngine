package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderPreprocessor(private val clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                             private val messageSender: MessageSender,
                             private val messageSequenceNumberHolder: MessageSequenceNumberHolder) : MessagePreprocessor {

    @Autowired
    private lateinit var singleLimitOrderContextParser: SingleLimitOrderContextParser

    @Autowired
    private lateinit var limitOrderInputValidator: LimitOrderInputValidator

    override fun preProcess(messageWrapper: MessageWrapper) {
        val singleLimitOrderParsedData = singleLimitOrderContextParser.parse(messageWrapper)
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitContext

        singleLimitContext.validationResult = isOrderValid(singleLimitOrderParsedData)
    }

    private fun isOrderValid(singleLimitOrderParsedData: SingleLimitOrderParsedData): OrderValidationResult {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitContext

        return if (singleLimitContext.limitOrder.type == LimitOrderType.LIMIT) {
            validateLimitOrder(singleLimitOrderParsedData)
        } else {
            validateStopOrder(singleLimitOrderParsedData)
        }
    }

    private fun validateLimitOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData): OrderValidationResult {
        try {
            limitOrderInputValidator.validateLimitOrder(singleLimitOrderParsedData)
        } catch (e: OrderValidationException) {
            OrderValidationResult(false, e.message, e.orderStatus)
        }
        return OrderValidationResult(true)
    }

    private fun validateStopOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData): OrderValidationResult {
        try {
            limitOrderInputValidator.validateStopOrder(singleLimitOrderParsedData)
        } catch (e: OrderValidationException) {
            return OrderValidationResult(false, e.message, e.orderStatus)
        }
        return OrderValidationResult(true)
    }

    private fun processStopOrderValidationFailure(singleLimitOrderParsedData: SingleLimitOrderParsedData, message: String, orderStatus: OrderStatus) {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitContext
        val limitOrder = singleLimitContext.limitOrder

        limitOrder.updateStatus(orderStatus, singleLimitContext.orderProcessingStartTime)

        val messageStatus = MessageStatusUtils.toMessageStatus(orderStatus)

    }

    private fun processLimitOrderValidationFailure(singleLimitOrderParsedData: SingleLimitOrderParsedData, reason: String, orderStatus: OrderStatus) {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitContext

        val limitOrder = singleLimitContext.limitOrder
        limitOrder.updateStatus(orderStatus, singleLimitContext.orderProcessingStartTime)
        if (!singleLimitContext.isTrustedClient) {
            val orders = mutableListOf(LimitOrderWithTrades(limitOrder))

            clientsLimitOrdersQueue.put(LimitOrdersReport(singleLimitContext.messageId, orders))
            messageSender.sendMessage(EventFactory.createExecutionEvent(messageSequenceNumberHolder.getNewValue(),
                    singleLimitContext.messageId,
                    limitOrder.externalId,
                    singleLimitContext.orderProcessingStartTime,
                    MessageType.LIMIT_ORDER,
                    //todo: check balance updates are empty if validation fails
                    emptyList(),
                    orders))
        }

        writeResponse(singleLimitOrderParsedData.messageWrapper,
                limitOrder,
                MessageStatusUtils.toMessageStatus(limitOrder.status),
                reason)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun writeResponse(messageWrapper: MessageWrapper?, order: LimitOrder, status: MessageStatus, reason: String? = null) {
        if (messageWrapper == null) {
            return
        }
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(order.externalId.toLong()))
        } else {
            if (reason == null) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setMatchingEngineId(order.id).setStatus(status.type))
            } else {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setMatchingEngineId(order.id).setStatus(status.type).setStatusReason(reason))
            }
        }
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        //do nothing
    }
}