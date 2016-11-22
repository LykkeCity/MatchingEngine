package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.ME_LIMIT_ORDER
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.JsonSerializable
import com.lykke.matching.engine.outgoing.OrderBook
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class SingleLimitOrderService(val limitOrderService: GenericLimitOrderService,
                              val orderBookQueue: BlockingQueue<JsonSerializable>,
                              val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>): AbsractService<ProtocolMessages.LimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, price: ${RoundingUtils.roundForPrint(message.price)}")

        val now = Date()
        val order = LimitOrder(UUID.randomUUID().toString(), message.assetPairId, message.clientId, message.volume,
                message.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, null, message.volume, null)

        if (message.cancelAllPreviousLimitOrders) {
            limitOrderService.cancelAllPreviousOrders(order.clientId, order.assetPairId, order.isBuySide)
        }

        limitOrderService.processLimitOrder(order)

        val orderBook = OrderBook(message.assetPairId, order.isBuySide, now, limitOrderService.getOrderBook(message.assetPairId).copy().getOrderBook(order.isBuySide))
        orderBookQueue.put(orderBook)
        rabbitOrderBookQueue.put(orderBook)

        LOGGER.info("Limit order id: ${message.uid}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} added to order book")
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())

        METRICS_LOGGER.log(KeyValue(ME_LIMIT_ORDER, (++messagesCount).toString()))
    }

    private fun parse(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }
}
