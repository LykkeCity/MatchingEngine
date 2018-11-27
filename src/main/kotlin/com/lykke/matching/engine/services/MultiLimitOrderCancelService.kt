package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.util.Date

@Component
class MultiLimitOrderCancelService(private val limitOrderService: GenericLimitOrderService,
                                   private val genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
                                   private val applicationSettingsHolder: ApplicationSettingsHolder,
                                   private val messageProcessingStatusHolder: MessageProcessingStatusHolder) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        LOGGER.debug("Got multi limit order cancel " +
                "message id: ${messageWrapper.messageId}, id: ${message.uid}, ${message.clientId}, " +
                "assetPair: ${message.assetPairId}, isBuy: ${message.isBuy}")

        if (!messageProcessingStatusHolder.isMessageProcessingEnabled(DisabledFunctionalityRule(null,
                        message.assetPairId,
                        MessageType.MULTI_LIMIT_ORDER_CANCEL))) {
            writeResponse(messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return
        }

        val now = Date()
        val ordersToCancel = limitOrderService.searchOrders(message.clientId, message.assetPairId, message.isBuy)
        if (ordersToCancel.isNotEmpty()) {
            val updated = genericLimitOrdersCancellerFactory.create(LOGGER, now)
                    .preProcessLimitOrders(ordersToCancel)
                    .applyFull(messageWrapper, message.uid, messageWrapper.messageId!!,
                            messageWrapper.processedMessage,
                            MessageType.MULTI_LIMIT_ORDER_CANCEL,
                            false)
            messageWrapper.triedToPersist = true
            messageWrapper.persisted = updated
            if (!updated) {
                LOGGER.debug("Unable to save result for multi limit order cancel id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, isBuy: ${message.isBuy}")
                messageWrapper.writeNewResponse(
                        ProtocolMessages.NewResponse.newBuilder()
                                .setStatus(MessageStatus.RUNTIME.type)
                                .setStatusReason("Unable to save result"))
                return
            }
        }
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(MessageStatus.OK.type))
        LOGGER.debug("Multi limit order cancel id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, isBuy: ${message.isBuy} processed")
    }

    private fun getMessage(messageWrapper: MessageWrapper) =
            messageWrapper.parsedMessage!! as ProtocolMessages.MultiLimitOrderCancel

    private fun parse(array: ByteArray): ProtocolMessages.MultiLimitOrderCancel {
        return ProtocolMessages.MultiLimitOrderCancel.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid
        messageWrapper.processedMessage = if (applicationSettingsHolder.isTrustedClient(message.clientId))
            null
        else
            ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }
}