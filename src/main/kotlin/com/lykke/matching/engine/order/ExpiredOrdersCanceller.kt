package com.lykke.matching.engine.order

import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
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
    }

    @Scheduled(fixedRateString = "\${expired.orders.cancel.interval}")
    fun cancelExpiredOrders() {
        try {
            val ordersToCancelExternalIds = expiryOrdersQueue.getExpiredOrderExternalIds(Date())
            if (ordersToCancelExternalIds.isEmpty()) {
                return
            }

            val messageId = UUID.randomUUID().toString()
            LOGGER.info("Generating message to cancel expired orders: messageId=$messageId, orders=$ordersToCancelExternalIds")

            val messageWrapper = MessageWrapper("localhost",
                    MessageType.LIMIT_ORDER_CANCEL.type,
                    generateMessage(messageId, ordersToCancelExternalIds),
                    null)
            messageRouter.preProcessedMessageQueue.put(messageWrapper)
        } catch (e: Exception) {
            val message = "Unable to cancel expired orders"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }

    private fun generateMessage(messageId: String,
                                orderIds: Collection<String>): ByteArray {
        return ProtocolMessages.LimitOrderCancel.newBuilder()
                .setUid(UUID.randomUUID().toString())
                .setMessageId(messageId)
                .addAllLimitOrderId(orderIds).build().toByteArray()
    }
}