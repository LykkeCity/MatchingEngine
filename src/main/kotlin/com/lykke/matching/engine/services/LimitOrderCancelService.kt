package com.lykke.matching.engine.services

import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.LIMIT_ORDER_ID
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_LIMIT_ORDER_CANCEL
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.MetricsLogger.Companion.DATE_TIME_FORMATTER
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.apache.log4j.Logger
import java.time.LocalDateTime

class LimitOrderCancelService(private val limitOrderService: LimitOrderService): AbsractService<ProtocolMessages.LimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got limit order cancel request id: ${message.uid}")

        limitOrderService.cancelLimitOrder(message.limitOrderId.toString())
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())

        METRICS_LOGGER.log(Line(ME_LIMIT_ORDER_CANCEL, arrayOf(
                KeyValue(UID, message.uid.toString()),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(DATE_TIME_FORMATTER)),
                KeyValue(LIMIT_ORDER_ID, message.limitOrderId.toString())
        )))
        METRICS_LOGGER.log(KeyValue(ME_LIMIT_ORDER_CANCEL, (++messagesCount).toString()))
    }

    private fun parse(array: ByteArray): ProtocolMessages.LimitOrderCancel {
        return ProtocolMessages.LimitOrderCancel.parseFrom(array)
    }
}
