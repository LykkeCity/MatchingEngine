package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.ME_LIMIT_ORDER
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class SingleLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                              private val orderBookQueue: BlockingQueue<OrderBook>,
                              private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                              private val assetsPairsHolder: AssetsPairsHolder,
                              private val negativeSpreadAssets: Set<String>): AbstractService<ProtocolMessages.OldLimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val order: LimitOrder

        val now = Date()
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val oldMessage = parseOldLimitOrder(messageWrapper.byteArray)
            val uid = UUID.randomUUID().toString()
            order = LimitOrder(uid, oldMessage.uid.toString(), oldMessage.assetPairId, oldMessage.clientId, oldMessage.volume,
                    oldMessage.price, OrderStatus.InOrderBook.name, Date(oldMessage.timestamp), now, oldMessage.volume, null)

            LOGGER.debug("Got limit order id: ${oldMessage.uid}, client ${oldMessage.clientId}, assetPair: ${oldMessage.assetPairId}, volume: ${RoundingUtils.roundForPrint(oldMessage.volume)}, price: ${RoundingUtils.roundForPrint(oldMessage.price)}")

            if (oldMessage.cancelAllPreviousLimitOrders) {
                limitOrderService.cancelAllPreviousOrders(order.clientId, order.assetPairId, order.isBuySide())
            }
        } else {
            val message = parseLimitOrder(messageWrapper.byteArray)
            val uid = UUID.randomUUID().toString()
            order = LimitOrder(uid, message.uid, message.assetPairId, message.clientId, message.volume,
                    message.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, message.volume, null)

            LOGGER.debug("Got limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, price: ${RoundingUtils.roundForPrint(message.price)}")

            if (message.cancelAllPreviousLimitOrders) {
                limitOrderService.cancelAllPreviousOrders(order.clientId, order.assetPairId, order.isBuySide())
            }
        }

        val book = limitOrderService.getOrderBook(order.assetPairId)

        val pair = assetsPairsHolder.getAssetPair(order.assetPairId)
        if ((negativeSpreadAssets.contains(pair.baseAssetId) || negativeSpreadAssets.contains(pair.quotingAssetId)) && book.leadToNegativeSpread(order)) {
            LOGGER.info("Limit order id: ${order.externalId}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} lead to negative spread, ignoring it")
        } else {
            limitOrderService.processLimitOrder(order)

            val orderBook = OrderBook(order.assetPairId, order.isBuySide(), now, limitOrderService.getOrderBook(order.assetPairId).copy().getOrderBook(order.isBuySide()))
            orderBookQueue.put(orderBook)
            rabbitOrderBookQueue.put(orderBook)

            LOGGER.info("Limit order id: ${order.externalId}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} added to order book")
        }

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(now.time).build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(MessageStatus.OK.type).build())
        }

        METRICS_LOGGER.log(KeyValue(ME_LIMIT_ORDER, (++messagesCount).toString()))
    }

    private fun parseLimitOrder(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun parseOldLimitOrder(array: ByteArray): ProtocolMessages.OldLimitOrder {
        return ProtocolMessages.OldLimitOrder.parseFrom(array)
    }
}
