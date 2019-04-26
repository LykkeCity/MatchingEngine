package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.incoming.preprocessor.impl.MultilimitOrderPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.socket.TestClientHandler
import java.math.BigDecimal
import java.util.*

class MessageBuilder(private var singleLimitOrderPreprocessor: SingleLimitOrderPreprocessor,
                     private val cashInOutContextParser: CashInOutContextParser,
                     private val cashTransferContextParser: CashTransferContextParser,
                     private val limitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData>,
                     private val limitOrderMassCancelOperationContextParser: ContextParser<LimitOrderMassCancelOperationParsedData>,
                     private val multilimitOrderPreprocessor: MultilimitOrderPreprocessor) {
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
                            fees: List<NewLimitOrderFeeInstruction> = listOf(),
                            previousExternalId: String? = null,
                            timeInForce: OrderTimeInForce? = null,
                            expiryTime: Date? = null): LimitOrder =
                LimitOrder(uid, uid, assetId, clientId, BigDecimal.valueOf(volume), BigDecimal.valueOf(price), status, registered, registered, registered, BigDecimal.valueOf(volume), null,
                        reservedVolume?.toBigDecimal(), fee, fees,
                        type, lowerLimitPrice?.toBigDecimal(), lowerPrice?.toBigDecimal(),
                        upperLimitPrice?.toBigDecimal(), upperPrice?.toBigDecimal(),
                        previousExternalId,
                        timeInForce,
                        expiryTime,
                        null,
                        null)

        fun buildMarketOrderWrapper(order: MarketOrder): MessageWrapper {
            val builder = ProtocolMessages.MarketOrder.newBuilder()
                    .setUid(UUID.randomUUID().toString())
                    .setTimestamp(order.createdAt.time)
                    .setClientId(order.clientId)
                    .setAssetPairId(order.assetPairId)
                    .setVolume(order.volume.toDouble())
                    .setStraight(order.straight)
            order.fee?.let {
                builder.setFee(buildFee(it))
            }
            order.fees?.forEach {
                builder.addFees(buildFee(it))
            }
            return MessageWrapper("Test", MessageType.MARKET_ORDER.type, builder
                    .build().toByteArray(), TestClientHandler()
            )
        }

        fun buildFee(fee: FeeInstruction): ProtocolMessages.Fee {
            val builder = ProtocolMessages.Fee.newBuilder().setType(fee.type.externalId)
            fee.size?.let {
                builder.size = it.toDouble()
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
                builder.takerSize = it.toDouble()
            }
            fee.sizeType?.let {
                builder.takerSizeType = it.externalId
            }
            fee.makerSize?.let {
                builder.makerSize = it.toDouble()
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
                builder.takerSize = it.toDouble()
            }
            fee.sizeType?.let {
                builder.takerSizeType = it.externalId
            }
            fee.makerSize?.let {
                builder.makerSize = it.toDouble()
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
                MarketOrder(rowKey, rowKey, assetId, clientId,
                        BigDecimal.valueOf(volume), null, status, registered, registered, Date(),
                        null, straight,
                        reservedVolume?.toBigDecimal(),
                        fee = fee, fees = fees)

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
            else return listOf(NewFeeInstruction(type, sizeType,
                    if (size != null) BigDecimal.valueOf(size) else null,
                    sourceClientId, targetClientId, assetIds))
        }

        fun buildFeeInstruction(type: FeeType? = null,
                                sizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                size: Double? = null,
                                sourceClientId: String? = null,
                                targetClientId: String? = null,
                                assetIds: List<String> = emptyList()): NewFeeInstruction? {
            return if (type == null) null
            else return NewFeeInstruction(type, sizeType,
                    if (size != null) BigDecimal.valueOf(size) else null,
                    sourceClientId, targetClientId, assetIds)
        }

        fun buildLimitOrderFeeInstruction(type: FeeType? = null,
                                          takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                          takerSize: Double? = null,
                                          makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                          makerSize: Double? = null,
                                          sourceClientId: String? = null,
                                          targetClientId: String? = null): LimitOrderFeeInstruction? {
            return if (type == null) null
            else return LimitOrderFeeInstruction(type, takerSizeType,
                    if (takerSize != null) BigDecimal.valueOf(takerSize) else null,
                    makerSizeType,
                    if (makerSize != null) BigDecimal.valueOf(makerSize) else null,
                    sourceClientId,
                    targetClientId)
        }

        fun buildLimitOrderFeeInstructions(type: FeeType? = null,
                                           takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                           takerSize: Double? = null,
                                           makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                           makerSize: Double? = null,
                                           sourceClientId: String? = null,
                                           targetClientId: String? = null,
                                           assetIds: List<String> = emptyList(),
                                           makerFeeModificator: Double? = null): List<NewLimitOrderFeeInstruction> {
            return if (type == null) listOf()
            else return listOf(NewLimitOrderFeeInstruction(type, takerSizeType,
                    if (takerSize != null) BigDecimal.valueOf(takerSize) else null,
                    makerSizeType,
                    if (makerSize != null) BigDecimal.valueOf(makerSize) else null,
                    sourceClientId, targetClientId, assetIds,
                    if (makerFeeModificator != null) BigDecimal.valueOf(makerFeeModificator) else null))
        }
    }

    fun buildTransferWrapper(fromClientId: String,
                             toClientId: String,
                             assetId: String,
                             amount: Double,
                             overdraftLimit: Double,
                             businessId: String = UUID.randomUUID().toString()
    ): MessageWrapper {
        return cashTransferContextParser.parse(MessageWrapper("Test", MessageType.CASH_TRANSFER_OPERATION.type, ProtocolMessages.CashTransferOperation.newBuilder()
                .setId(businessId)
                .setFromClientId(fromClientId)
                .setToClientId(toClientId)
                .setAssetId(assetId)
                .setVolume(amount)
                .setOverdraftLimit(overdraftLimit)
                .setTimestamp(Date().time).build().toByteArray(), null)).messageWrapper
    }

    fun buildCashInOutWrapper(clientId: String, assetId: String, amount: Double, businessId: String = UUID.randomUUID().toString(),
                              fees: List<NewFeeInstruction> = listOf()): MessageWrapper {
        val builder = ProtocolMessages.CashInOutOperation.newBuilder()
                .setId(businessId)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setVolume(amount)
                .setTimestamp(Date().time)
        fees.forEach {
            builder.addFees(MessageBuilder.buildFee(it))
        }

        return cashInOutContextParser.parse(MessageWrapper("Test", MessageType.CASH_IN_OUT_OPERATION.type, builder.build().toByteArray(), null)).messageWrapper
    }

    fun buildLimitOrderCancelWrapper(uid: String) = buildLimitOrderCancelWrapper(listOf(uid))

    fun buildLimitOrderCancelWrapper(uids: List<String>): MessageWrapper {
        val parsedData = limitOrderCancelOperationContextParser.parse(MessageWrapper("Test", MessageType.LIMIT_ORDER_CANCEL.type, ProtocolMessages.LimitOrderCancel.newBuilder()
                .setUid(UUID.randomUUID().toString()).addAllLimitOrderId(uids).build().toByteArray(), null))
        return parsedData.messageWrapper
    }

    fun buildLimitOrderMassCancelWrapper(clientId: String? = null,
                                         assetPairId: String? = null,
                                         isBuy: Boolean? = null): MessageWrapper {
        val builder = ProtocolMessages.LimitOrderMassCancel.newBuilder()
                .setUid(UUID.randomUUID().toString())
        clientId?.let {
            builder.setClientId(it)
        }
        assetPairId?.let {
            builder.setAssetPairId(it)
        }
        isBuy?.let {
            builder.setIsBuy(it)
        }

        val messageWrapper = MessageWrapper("Test", MessageType.LIMIT_ORDER_MASS_CANCEL.type, builder.build().toByteArray(), null)
        return limitOrderMassCancelOperationContextParser.parse(messageWrapper).messageWrapper
    }

    private fun buildMultiLimitOrder(assetPairId: String,
                                     clientId: String,
                                     orders: List<IncomingLimitOrder>,
                                     cancel: Boolean,
                                     cancelMode: OrderCancelMode?): ProtocolMessages.MultiLimitOrder {
        val multiOrderBuilder = ProtocolMessages.MultiLimitOrder.newBuilder()
                .setUid(UUID.randomUUID().toString())
                .setTimestamp(Date().time)
                .setClientId(clientId)
                .setAssetPairId(assetPairId)
                .setCancelAllPreviousLimitOrders(cancel)
        cancelMode?.let { multiOrderBuilder.cancelMode = it.externalId }
        orders.forEach { order ->
            val orderBuilder = ProtocolMessages.MultiLimitOrder.Order.newBuilder()
                    .setVolume(order.volume)
            order.price?.let { orderBuilder.price = it }
            order.feeInstruction?.let { orderBuilder.fee = buildLimitOrderFee(it) }
            order.feeInstructions.forEach { orderBuilder.addFees(buildNewLimitOrderFee(it)) }
            orderBuilder.uid = order.uid
            order.oldUid?.let { orderBuilder.oldUid = order.oldUid }
            order.type?.let { orderBuilder.type = it.externalId }
            order.lowerLimitPrice?.let { orderBuilder.lowerLimitPrice = it }
            order.lowerPrice?.let { orderBuilder.lowerPrice = it }
            order.timeInForce?.let { orderBuilder.timeInForce = it.externalId }
            order.expiryTime?.let { orderBuilder.expiryTime = it.time }
            order.upperLimitPrice?.let { orderBuilder.upperLimitPrice = it }
            order.upperPrice?.let { orderBuilder.upperPrice = it }
            multiOrderBuilder.addOrders(orderBuilder.build())
        }
        return multiOrderBuilder.build()
    }

    @Deprecated("Use buildMultiLimitOrderWrapper(5)")
    fun buildMultiLimitOrderWrapper(pair: String,
                                    clientId: String,
                                    volumes: List<VolumePrice>,
                                    ordersFee: List<LimitOrderFeeInstruction> = emptyList(),
                                    ordersFees: List<List<NewLimitOrderFeeInstruction>> = emptyList(),
                                    ordersUid: List<String> = emptyList(),
                                    cancel: Boolean = false,
                                    cancelMode: OrderCancelMode? = null
    ): MessageWrapper {
        val orders = volumes.mapIndexed { i, volume ->
            IncomingLimitOrder(volume.volume.toDouble(),
                    volume.price.toDouble(),
                    if (i < ordersUid.size) ordersUid[i] else UUID.randomUUID().toString(),
                    if (i < ordersFee.size) ordersFee[i] else null,
                    if (i < ordersFees.size) ordersFees[i] else emptyList(),
                    null)
        }
        return buildMultiLimitOrderWrapper(pair, clientId, orders, cancel, cancelMode)
    }

    fun buildMultiLimitOrderWrapper(pair: String,
                                    clientId: String,
                                    orders: List<IncomingLimitOrder>,
                                    cancel: Boolean = true,
                                    cancelMode: OrderCancelMode? = null
    ): MessageWrapper {
        val messageWrapper = MessageWrapper("Test", MessageType.MULTI_LIMIT_ORDER.type, buildMultiLimitOrder(pair, clientId,
                orders,
                cancel,
                cancelMode).toByteArray(), TestClientHandler(), messageId = "test", id = "test")
        multilimitOrderPreprocessor.preProcess(messageWrapper)
        return messageWrapper
    }


    fun buildLimitOrderWrapper(order: LimitOrder,
                               cancel: Boolean = false): MessageWrapper {
        val builder = ProtocolMessages.LimitOrder.newBuilder()
                .setUid(order.externalId)
                .setTimestamp(order.createdAt.time)
                .setClientId(order.clientId)
                .setAssetPairId(order.assetPairId)
                .setVolume(order.volume.toDouble())
                .setCancelAllPreviousLimitOrders(cancel)
                .setType(order.type!!.externalId)
        if (order.type == LimitOrderType.LIMIT) {
            builder.price = order.price.toDouble()
        }
        order.fee?.let {
            builder.setFee(buildLimitOrderFee(it as LimitOrderFeeInstruction))
        }
        order.fees?.forEach {
            builder.addFees(buildNewLimitOrderFee(it as NewLimitOrderFeeInstruction))
        }
        order.lowerLimitPrice?.let { builder.setLowerLimitPrice(it.toDouble()) }
        order.lowerPrice?.let { builder.setLowerPrice(it.toDouble()) }
        order.upperLimitPrice?.let { builder.setUpperLimitPrice(it.toDouble()) }
        order.upperPrice?.let { builder.setUpperPrice(it.toDouble()) }
        order.expiryTime?.let { builder.setExpiryTime(it.time) }
        order.timeInForce?.let { builder.setTimeInForce(it.externalId) }
        val messageWrapper = MessageWrapper("Test", MessageType.LIMIT_ORDER.type, builder.build().toByteArray(), TestClientHandler(), messageId = "test", id = "test")
        singleLimitOrderPreprocessor.preProcess(messageWrapper)

        return messageWrapper
    }
}