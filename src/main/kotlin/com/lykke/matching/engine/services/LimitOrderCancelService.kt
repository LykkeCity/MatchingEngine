package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.LIMIT_ORDER_ID
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_LIMIT_ORDER_CANCEL
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.MetricsLogger.Companion.DATE_TIME_FORMATTER
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.concurrent.BlockingQueue

class LimitOrderCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                              private val limitOrderReportQueue: BlockingQueue<JsonSerializable>): AbstractService<ProtocolMessages.OldLimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val order: LimitOrder?
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            val message = parseOldLimitOrderCancel(messageWrapper.byteArray)
            LOGGER.debug("Got limit order (id: ${message.limitOrderId}) cancel request id: ${message.uid}")

            order = genericLimitOrderService.cancelLimitOrder(message.limitOrderId.toString())
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())

            METRICS_LOGGER.log(Line(ME_LIMIT_ORDER_CANCEL, arrayOf(
                    KeyValue(UID, message.uid.toString()),
                    KeyValue(TIMESTAMP, LocalDateTime.now().format(DATE_TIME_FORMATTER)),
                    KeyValue(LIMIT_ORDER_ID, message.limitOrderId.toString())
            )))
            METRICS_LOGGER.log(KeyValue(ME_LIMIT_ORDER_CANCEL, (++messagesCount).toString()))
        } else {
            val message = parseLimitOrderCancel(messageWrapper.byteArray)
            LOGGER.debug("Got limit order (id: ${message.limitOrderId}) cancel request id: ${message.uid}")

            order = genericLimitOrderService.cancelLimitOrder(message.limitOrderId)
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.OK.type).build())

            METRICS_LOGGER.log(Line(ME_LIMIT_ORDER_CANCEL, arrayOf(
                    KeyValue(UID, message.uid.toString()),
                    KeyValue(TIMESTAMP, LocalDateTime.now().format(DATE_TIME_FORMATTER)),
                    KeyValue(LIMIT_ORDER_ID, message.limitOrderId)
            )))
            METRICS_LOGGER.log(KeyValue(ME_LIMIT_ORDER_CANCEL, (++messagesCount).toString()))
        }

        if (order != null) {
            val report = LimitOrdersReport()
            report.orders.add(LimitOrderWithTrades(order))
            limitOrderReportQueue.put(report)
        }
    }

    private fun parseOldLimitOrderCancel(array: ByteArray): ProtocolMessages.OldLimitOrderCancel {
        return ProtocolMessages.OldLimitOrderCancel.parseFrom(array)
    }

    private fun parseLimitOrderCancel(array: ByteArray): ProtocolMessages.LimitOrderCancel {
        return ProtocolMessages.LimitOrderCancel.parseFrom(array)
    }
}
