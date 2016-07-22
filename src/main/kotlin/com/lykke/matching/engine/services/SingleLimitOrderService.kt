package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.logging.AMOUNT
import com.lykke.matching.engine.logging.ASSET_PAIR
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_LIMIT_ORDER
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.MetricsLogger.Companion.DATE_TIME_FORMATTER
import com.lykke.matching.engine.logging.PRICE
import com.lykke.matching.engine.logging.STATUS
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Date
import java.util.UUID

class SingleLimitOrderService(val limitOrderService: GenericLimitOrderService): AbsractService<ProtocolMessages.LimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, volume: ${message.volume}, price: ${message.price}")

        val order = LimitOrder(UUID.randomUUID().toString(), message.assetPairId, message.clientId, message.volume,
                message.price, OrderStatus.InOrderBook.name, Date(message.timestamp), Date(), null, message.volume, null)

        if (message.cancelAllPreviousLimitOrders) {
            limitOrderService.cancelAllPreviousOrders(order.clientId, order.assetPairId, order.isBuySide)
        }

        limitOrderService.processLimitOrder(order)

        LOGGER.info("Limit order id: ${message.uid}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${order.volume}, price: ${order.price} added to order book")
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())

        METRICS_LOGGER.log(Line(ME_LIMIT_ORDER, arrayOf(
                KeyValue(UID, message.uid.toString()),
                KeyValue(ID, order.id),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, order.clientId),
                KeyValue(ASSET_PAIR, order.assetPairId),
                KeyValue(AMOUNT, order.volume.toString()),
                KeyValue(PRICE, order.price.toString()),
                KeyValue(STATUS, order.status)
        )))
        METRICS_LOGGER.log(KeyValue(ME_LIMIT_ORDER, (++messagesCount).toString()))
    }

    private fun parse(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }
}
