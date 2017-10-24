package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
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
                            volume:Double = 1000.0,
                            fee: LimitOrderFeeInstruction? = null): NewLimitOrder =
                NewLimitOrder(uid, uid, assetId, clientId, volume, price, status, registered, registered, volume, null, fee = fee)

        fun buildMarketOrderWrapper(order: MarketOrder): MessageWrapper {
            val builder = ProtocolMessages.MarketOrder.newBuilder()
                    .setUid(UUID.randomUUID().toString())
                    .setTimestamp(order.createdAt.time)
                    .setClientId(order.clientId)
                    .setAssetPairId(order.assetPairId)
                    .setVolume(order.volume)
                    .setStraight(order.straight)
            order.fee?.let {
                builder.setFee(buildFee(it))
            }
            return MessageWrapper("Test", MessageType.MARKET_ORDER.type, builder
                    .build().toByteArray(), null)
        }

        private fun buildFeeCommon(fee: FeeInstruction): ProtocolMessages.FeeCommon {
            val builder = ProtocolMessages.FeeCommon.newBuilder().setType(ProtocolMessages.FeeType.valueOf(fee.type.externalId))
            fee.sourceClientId?.let {
                builder.setSourceClientId(it)
            }
            fee.targetClientId?.let {
                builder.setTargetClientId(it)
            }
            return builder.build()
        }

        private fun buildFee(fee: FeeInstruction?): ProtocolMessages.Fee? {
            if (fee == null) {
                return null
            }
            val builder = ProtocolMessages.Fee.newBuilder().setFeeCommon(buildFeeCommon(fee))
            fee.size?.let {
                builder.size = it
            }
            return builder.build()
        }

        fun buildLimitOrderFee(fee: LimitOrderFeeInstruction?): ProtocolMessages.LimitOrderFee? {
            if (fee == null) {
                return null
            }
            val builder = ProtocolMessages.LimitOrderFee.newBuilder().setFeeCommon(buildFeeCommon(fee))
            fee.size?.let {
                builder.takerSize = it
            }
            fee.makerSize?.let {
                builder.makerSize = it
            }
            return builder.build()
        }

        fun buildMarketOrder(rowKey: String = UUID.randomUUID().toString(),
                             assetId: String = "EURUSD",
                             clientId: String = "Client1",
                             registered: Date = Date(),
                             status: String = OrderStatus.InOrderBook.name,
                             straight: Boolean = true,
                             volume: Double = 1000.0,
                             fee: FeeInstruction? = null): MarketOrder =
                MarketOrder(rowKey, rowKey, assetId, clientId, volume, null, status, registered, Date(), null, straight, fee = fee)

        fun buildLimitOrderWrapper(order: NewLimitOrder,
                                   cancel: Boolean = false): MessageWrapper {
            val builder = ProtocolMessages.LimitOrder.newBuilder()
                    .setUid(order.externalId)
                    .setTimestamp(order.createdAt.time)
                    .setClientId(order.clientId)
                    .setAssetPairId(order.assetPairId)
                    .setVolume(order.volume)
                    .setPrice(order.price).setCancelAllPreviousLimitOrders(cancel)
            order.fee?.let {
                builder.setFee(buildLimitOrderFee(it as LimitOrderFeeInstruction))
            }
            return MessageWrapper("Test", MessageType.LIMIT_ORDER.type, builder.build().toByteArray(), null)
        }

         fun buildLimitOrderCancelWrapper(uid: String): MessageWrapper = MessageWrapper("Test", MessageType.LIMIT_ORDER_CANCEL.type, ProtocolMessages.LimitOrderCancel.newBuilder()
                    .setUid(UUID.randomUUID().toString()).setLimitOrderId(uid).build().toByteArray(), null)
    }
}