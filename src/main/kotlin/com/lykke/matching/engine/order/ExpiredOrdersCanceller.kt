package com.lykke.matching.engine.order

import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.Date
import java.util.UUID

@Component
class ExpiredOrdersCanceller(private val expiryOrdersQueue: ExpiryOrdersQueue,
                             private val messageRouter: MessageRouter) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(ExpiredOrdersCanceller::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private val MESSAGE_TYPE = MessageType.LIMIT_ORDER_CANCEL
    }

    @Scheduled(fixedRateString = "\${expired.orders.cancel.interval}")
    fun cancelExpiredOrders() {
        try {
            val now = Date()
            val ordersExternalIdsToCancel = expiryOrdersQueue.getExpiredOrdersExternalIds(now)
            if (ordersExternalIdsToCancel.isEmpty()) {
                return
            }

            val messageId = UUID.randomUUID().toString()
            val requestId = UUID.randomUUID().toString()
            LOGGER.info("Generating message to cancel expired orders: messageId=$messageId, requestId=$requestId, date=$now, orders=$ordersExternalIdsToCancel")

            val messageWrapper = createMessageWrapper(messageId,
                    requestId,
                    now,
                    ordersExternalIdsToCancel)

            messageRouter.preProcessedMessageQueue.put(messageWrapper)
        } catch (e: Exception) {
            val message = "Unable to cancel expired orders"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }

    private fun createMessageWrapper(messageId: String,
                                     requestId: String,
                                     date: Date,
                                     ordersExternalIds: Collection<String>): MessageWrapper {
        return MessageWrapper("localhost",
                MESSAGE_TYPE.type,
                ByteArray(0),
                null,
                messageId = messageId,
                id = requestId,
                context = createOperationContext(messageId,
                        requestId,
                        date,
                        ordersExternalIds))
    }

    private fun createOperationContext(messageId: String,
                                       requestId: String,
                                       date: Date,
                                       ordersExternalIds: Collection<String>): LimitOrderCancelOperationContext {
        return LimitOrderCancelOperationContext(requestId,
                messageId,
                createProcessedMessage(messageId, date),
                ordersExternalIds.toSet(),
                MESSAGE_TYPE)
    }

    private fun createProcessedMessage(messageId: String,
                                       date: Date): ProcessedMessage {
        return ProcessedMessage(MESSAGE_TYPE.type,
                date.time,
                messageId)
    }
}