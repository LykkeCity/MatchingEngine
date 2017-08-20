package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MarketOrder
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
                            volume:Double = 1000.0): LimitOrder =
                LimitOrder(uid, uid, assetId, clientId, volume, price, status, registered, registered, volume, null)

        fun buildMarketOrderWrapper(order: MarketOrder): MessageWrapper {
            return MessageWrapper("Test", MessageType.OLD_MARKET_ORDER.type, ProtocolMessages.OldMarketOrder.newBuilder()
                    .setUid(Date().time)
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

        fun buildLimitOrderWrapper(order: LimitOrder,
                                   cancel: Boolean = false,
                                   uid: Long = Date().time) =
                MessageWrapper("Test", MessageType.OLD_LIMIT_ORDER.type, ProtocolMessages.OldLimitOrder.newBuilder()
                        .setUid(uid)
                        .setTimestamp(order.createdAt.time)
                        .setClientId(order.clientId)
                        .setAssetPairId(order.assetPairId)
                        .setVolume(order.volume)
                        .setPrice(order.price).setCancelAllPreviousLimitOrders(cancel).build().toByteArray(), null)
    }
}