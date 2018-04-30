package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
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
                            volume: Double = 1000.0,
                            type: LimitOrderType = LimitOrderType.LIMIT,
                            lowerLimitPrice: Double? = null,
                            lowerPrice: Double? = null,
                            upperLimitPrice: Double? = null,
                            upperPrice: Double? = null,
                            reservedVolume: Double? = null,
                            fee: LimitOrderFeeInstruction? = null,
                            fees: List<NewLimitOrderFeeInstruction> = listOf()): NewLimitOrder =
                NewLimitOrder(uid, uid, assetId, clientId, volume, price, status, registered, registered, volume, null, reservedVolume, fee, fees,
                        type, lowerLimitPrice, lowerPrice, upperLimitPrice, upperPrice)

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
            order.fees?.forEach {
                builder.addFees(buildFee(it))
            }
            return MessageWrapper("Test", MessageType.MARKET_ORDER.type, builder
                    .build().toByteArray(), null)
        }

        fun buildFee(fee: FeeInstruction): ProtocolMessages.Fee {
            val builder = ProtocolMessages.Fee.newBuilder().setType(fee.type.externalId)
            fee.size?.let {
                builder.size = it
            }
            fee.sourceClientId?.let {
                builder.setSourceClientId(it)
            }
            fee.targetClientId?.let {
                builder.setTargetClientId(it)
            }
            fee.sizeType?.let {
                builder.setSizeType(it.externalId)
            }
            if (fee is NewFeeInstruction) {
                builder.addAllAssetId(fee.assetIds)
            }
            return builder.build()
        }

        fun buildLimitOrderFee(fee: LimitOrderFeeInstruction): ProtocolMessages.LimitOrderFee {
            val builder = ProtocolMessages.LimitOrderFee.newBuilder().setType(fee.type.externalId)
            fee.size?.let {
                builder.takerSize = it
            }
            fee.sizeType?.let {
                builder.takerSizeType = it.externalId
            }
            fee.makerSize?.let {
                builder.makerSize = it
            }
            fee.makerSizeType?.let {
                builder.makerSizeType = it.externalId
            }
            fee.sourceClientId?.let {
                builder.setSourceClientId(it)
            }
            fee.targetClientId?.let {
                builder.setTargetClientId(it)
            }
            return builder.build()
        }

        fun buildNewLimitOrderFee(fee: NewLimitOrderFeeInstruction): ProtocolMessages.LimitOrderFee {
            val builder = ProtocolMessages.LimitOrderFee.newBuilder().setType(fee.type.externalId)
            fee.size?.let {
                builder.takerSize = it
            }
            fee.sizeType?.let {
                builder.takerSizeType = it.externalId
            }
            fee.makerSize?.let {
                builder.makerSize = it
            }
            fee.makerSizeType?.let {
                builder.makerSizeType = it.externalId
            }
            fee.sourceClientId?.let {
                builder.setSourceClientId(it)
            }
            fee.targetClientId?.let {
                builder.setTargetClientId(it)
            }
            builder.addAllAssetId(fee.assetIds)
            return builder.build()
        }

        fun buildMarketOrder(rowKey: String = UUID.randomUUID().toString(),
                             assetId: String = "EURUSD",
                             clientId: String = "Client1",
                             registered: Date = Date(),
                             status: String = OrderStatus.InOrderBook.name,
                             straight: Boolean = true,
                             volume: Double = 1000.0,
                             reservedVolume: Double? = null,
                             fee: FeeInstruction? = null,
                             fees: List<NewFeeInstruction> = listOf()): MarketOrder =
                MarketOrder(rowKey, rowKey, assetId, clientId, volume, null, status, registered, Date(), null, straight, reservedVolume, fee = fee, fees = fees)

        fun buildLimitOrderWrapper(order: NewLimitOrder,
                                   cancel: Boolean = false): MessageWrapper {
            val builder = ProtocolMessages.LimitOrder.newBuilder()
                    .setUid(order.externalId)
                    .setTimestamp(order.createdAt.time)
                    .setClientId(order.clientId)
                    .setAssetPairId(order.assetPairId)
                    .setVolume(order.volume)
                    .setCancelAllPreviousLimitOrders(cancel)
                    .setType(order.type!!.externalId)
            if (order.type == LimitOrderType.LIMIT) {
                builder.price = order.price
            }
            order.fee?.let {
                builder.setFee(buildLimitOrderFee(it as LimitOrderFeeInstruction))
            }
            order.fees?.forEach {
                builder.addFees(buildNewLimitOrderFee(it as NewLimitOrderFeeInstruction))
            }
            order.lowerLimitPrice?.let { builder.setLowerLimitPrice(it) }
            order.lowerPrice?.let { builder.setLowerPrice(it) }
            order.upperLimitPrice?.let { builder.setUpperLimitPrice(it) }
            order.upperPrice?.let { builder.setUpperPrice(it) }
            return MessageWrapper("Test", MessageType.LIMIT_ORDER.type, builder.build().toByteArray(), null)
        }

        fun buildMultiLimitOrderWrapper(pair: String,
                                                clientId: String,
                                                volumes: List<VolumePrice>,
                                                ordersFee: List<LimitOrderFeeInstruction>,
                                                ordersFees: List<List<NewLimitOrderFeeInstruction>>,
                                                ordersUid: List<String> = emptyList(),
                                                cancel: Boolean = false): MessageWrapper {
            return MessageWrapper("Test", MessageType.MULTI_LIMIT_ORDER.type, buildMultiLimitOrder(pair, clientId, volumes, ordersFee, ordersFees, ordersUid, cancel).toByteArray(), null)
        }

        private fun buildMultiLimitOrder(assetPairId: String,
                                         clientId: String,
                                         volumes: List<VolumePrice>,
                                         ordersFee: List<LimitOrderFeeInstruction>,
                                         ordersFees: List<List<NewLimitOrderFeeInstruction>>,
                                         ordersUid: List<String>,
                                         cancel: Boolean): ProtocolMessages.MultiLimitOrder {
            val multiOrderBuilder = ProtocolMessages.MultiLimitOrder.newBuilder()
                    .setUid(UUID.randomUUID().toString())
                    .setTimestamp(Date().time)
                    .setClientId(clientId)
                    .setAssetPairId(assetPairId)
                    .setCancelAllPreviousLimitOrders(cancel)
            volumes.forEachIndexed { index, volume ->
                val orderBuilder = ProtocolMessages.MultiLimitOrder.Order.newBuilder()
                        .setVolume(volume.volume)
                        .setPrice(volume.price)
                if (ordersFee.size > index) {
                    orderBuilder.fee = buildLimitOrderFee(ordersFee[index])
                }
                if (ordersFees.size > index) {
                    ordersFees[index].forEach {
                        orderBuilder.addFees(buildNewLimitOrderFee(it))
                    }
                }
                if (ordersUid.size > index) {
                    orderBuilder.uid = ordersUid[index]
                } else {
                    orderBuilder.uid = UUID.randomUUID().toString()
                }
                multiOrderBuilder.addOrders(orderBuilder.build())
            }
            return multiOrderBuilder.build()
        }

         fun buildLimitOrderCancelWrapper(uid: String): MessageWrapper = MessageWrapper("Test", MessageType.LIMIT_ORDER_CANCEL.type, ProtocolMessages.LimitOrderCancel.newBuilder()
                    .setUid(UUID.randomUUID().toString()).setLimitOrderId(uid).build().toByteArray(), null)

        fun buildMultiLimitOrderCancelWrapper(clientId: String, assetPairId: String, isBuy: Boolean): MessageWrapper = MessageWrapper("Test", MessageType.MULTI_LIMIT_ORDER_CANCEL.type, ProtocolMessages.MultiLimitOrderCancel.newBuilder()
                .setUid(UUID.randomUUID().toString())
                .setTimestamp(Date().time)
                .setClientId(clientId)
                .setAssetPairId(assetPairId)
                .setIsBuy(isBuy).build().toByteArray(), null)

        fun buildFeeInstructions(type: FeeType? = null,
                                 sizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                 size: Double? = null,
                                 sourceClientId: String? = null,
                                 targetClientId: String? = null,
                                 assetIds: List<String> = emptyList()): List<NewFeeInstruction> {
            return if (type == null) listOf()
            else return listOf(NewFeeInstruction(type, sizeType, size, sourceClientId, targetClientId, assetIds))
        }

        fun buildFeeInstruction(type: FeeType? = null,
                                sizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                size: Double? = null,
                                sourceClientId: String? = null,
                                targetClientId: String? = null,
                                assetIds: List<String> = emptyList()): NewFeeInstruction? {
            return if (type == null) null
            else return NewFeeInstruction(type, sizeType, size, sourceClientId, targetClientId, assetIds)
        }

        fun buildLimitOrderFeeInstruction(type: FeeType? = null,
                                          takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                          takerSize: Double? = null,
                                          makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                          makerSize: Double? = null,
                                          sourceClientId: String? = null,
                                          targetClientId: String? = null): LimitOrderFeeInstruction? {
            return if (type == null) null
            else return LimitOrderFeeInstruction(type, takerSizeType, takerSize, makerSizeType, makerSize, sourceClientId, targetClientId)
        }

        fun buildLimitOrderFeeInstructions(type: FeeType? = null,
                                           takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                           takerSize: Double? = null,
                                           makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                           makerSize: Double? = null,
                                           sourceClientId: String? = null,
                                           targetClientId: String? = null,
                                           makerFeeModificator: Double? = null): List<NewLimitOrderFeeInstruction> {
            return if (type == null) listOf()
            else return listOf(NewLimitOrderFeeInstruction(type, takerSizeType, takerSize, makerSizeType, makerSize, sourceClientId, targetClientId, listOf(), makerFeeModificator))
        }

        fun buildTransferWrapper(fromClientId: String,
                                 toClientId: String,
                                 assetId: String,
                                 amount: Double,
                                 overdraftLimit: Double,
                                 bussinesId: String = UUID.randomUUID().toString()
        ): MessageWrapper {
            return MessageWrapper("Test", MessageType.CASH_TRANSFER_OPERATION.type, ProtocolMessages.CashTransferOperation.newBuilder()
                    .setId(bussinesId)
                    .setFromClientId(fromClientId)
                    .setToClientId(toClientId)
                    .setAssetId(assetId)
                    .setVolume(amount)
                    .setOverdraftLimit(overdraftLimit)
                    .setTimestamp(Date().time).build().toByteArray(), null)
        }
    }
}