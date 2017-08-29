package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import java.util.Date
import java.util.UUID

class MessageBuilder {
    companion object {
        fun buildLimitOrder(uid: String = UUID.randomUUID().toString(),
                            assetId: String = "EURUSD",
                            clientId: String = "Client1",
                            price: Double = 100.0,
                            registered: Date = Date(),
                            status: String = OrderStatus.InOrderBook.name,
                            volume:Double = 1000.0): NewLimitOrder =
                NewLimitOrder(uid, uid, assetId, clientId, volume, price, status, registered, registered, volume, null)

        fun buildMarketOrderWrapper(order: MarketOrder): MessageWrapper {
            return MessageWrapper("Test", MessageType.MARKET_ORDER.type, ProtocolMessages.MarketOrder.newBuilder()
                    .setUid(UUID.randomUUID().toString())
                    .setTimestamp(order.createdAt.time)
                    .setClientId(order.clientId)
                    .setAssetPairId(order.assetPairId)
                    .setVolume(order.volume)
                    .setStraight(order.straight)
                    .build().toByteArray(), null)
        }

        fun buildMarketOrder(rowKey: String = UUID.randomUUID().toString(),
                             assetId: String = "EURUSD",
                             clientId: String = "Client1",
                             registered: Date = Date(),
                             status: String = OrderStatus.InOrderBook.name,
                             straight: Boolean = true,
                             volume: Double = 1000.0): MarketOrder =
                MarketOrder(rowKey, rowKey, assetId, clientId, volume, null, status, registered, Date(), null, straight)

        fun buildLimitOrderWrapper(order: NewLimitOrder,
                                   cancel: Boolean = false) =
                MessageWrapper("Test", MessageType.LIMIT_ORDER.type, ProtocolMessages.LimitOrder.newBuilder()
                        .setUid(order.externalId)
                        .setTimestamp(order.createdAt.time)
                        .setClientId(order.clientId)
                        .setAssetPairId(order.assetPairId)
                        .setVolume(order.volume)
                        .setPrice(order.price).setCancelAllPreviousLimitOrders(cancel).build().toByteArray(), null)

         fun buildLimitOrderCancelWrapper(uid: String): MessageWrapper = MessageWrapper("Test", MessageType.LIMIT_ORDER_CANCEL.type, ProtocolMessages.LimitOrderCancel.newBuilder()
                    .setUid(UUID.randomUUID().toString()).setLimitOrderId(uid).build().toByteArray(), null)
    }
}