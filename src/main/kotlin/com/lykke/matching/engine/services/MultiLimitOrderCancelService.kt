package com.lykke.matching.engine.services

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.process.common.CancelRequest
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.Date

@Service
class MultiLimitOrderCancelService(private val limitOrderService: GenericLimitOrderService,
                                   private val limitOrdersCancelServiceHelper: LimitOrdersCancelServiceHelper,
                                   private val applicationSettingsHolder: ApplicationSettingsHolder,
                                   private val performanceStatsHolder: PerformanceStatsHolder) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        LOGGER.debug("Got multi limit order cancel " +
                "message id: ${messageWrapper.messageId}, id: ${message.uid}, ${message.clientId}, " +
                "assetPair: ${message.assetPairId}, isBuy: ${message.isBuy}")

        val now = Date()
        val ordersToCancel = limitOrderService.searchOrders(message.clientId, message.assetPairId, message.isBuy)
        if (ordersToCancel.isEmpty()) {
            writeResponse(messageWrapper, MessageStatus.OK)
            return
        }
        val updateSuccessful = limitOrdersCancelServiceHelper.cancelOrdersAndWriteResponse(CancelRequest(ordersToCancel,
                emptyList(),
                message.assetPairId,
                messageWrapper.messageId!!,
                message.uid,
                MessageType.MULTI_LIMIT_ORDER_CANCEL,
                now,
                messageWrapper.processedMessage,
                messageWrapper,
                LOGGER))

        if (updateSuccessful) {
            LOGGER.debug("Multi limit order cancel id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, isBuy: ${message.isBuy} processed")
        }
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
        val start = System.nanoTime()
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
        val end = System.nanoTime()
        performanceStatsHolder.addWriteResponseTime(MessageType.MULTI_LIMIT_ORDER_CANCEL.type, end - start)
    }
}