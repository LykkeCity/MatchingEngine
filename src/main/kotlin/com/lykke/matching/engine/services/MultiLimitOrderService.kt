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
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Date
import java.util.HashSet
import java.util.UUID

class MultiLimitOrderService(val limitOrderService: GenericLimitOrderService): AbsractService<ProtocolMessages.MultiLimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got multi limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}")

        val orders = HashSet<LimitOrder>()
        var cancelBuySide = false
        var cancelSellSide = false

        message.ordersList.forEach { currentOrder ->
            orders.add(LimitOrder(UUID.randomUUID().toString(), message.assetPairId, message.clientId, currentOrder.volume,
                    currentOrder.price, OrderStatus.InOrderBook.name, Date(message.timestamp), Date(), null, currentOrder.volume, null))

            if (message.cancelAllPreviousLimitOrders) {
                if (currentOrder.volume > 0) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }

        if (message.cancelAllPreviousLimitOrders) {
            if (cancelBuySide) {
                limitOrderService.cancelAllPreviousOrders(message.clientId, message.assetPairId, true)
            }
            if (cancelSellSide) {
                limitOrderService.cancelAllPreviousOrders(message.clientId, message.assetPairId, false)
            }
        }

        orders.forEach { order ->
            limitOrderService.processLimitOrder(order)
            LOGGER.info("Part of multi limit order id: ${message.uid}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} added to order book")

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
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
    }

    private fun parse(array: ByteArray): ProtocolMessages.MultiLimitOrder {
        return ProtocolMessages.MultiLimitOrder.parseFrom(array)
    }
}
