package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.utils.NumberUtils

class SingleLimitOrderContext(val uid: String?,
                              val messageId: String,
                              var limitOrder: LimitOrder,
                              val isCancelOrders: Boolean,
                              val assetPair: AssetPair,
                              val baseAsset: Asset,
                              val quotingAsset: Asset,
                              val baseAssetDisabled: Boolean,
                              val quotingAssetDisabled: Boolean,
                              val limitAsset: Asset,
                              val isTrustedClient: Boolean,
                              val processedMessage: ProcessedMessage?,
                              var validationResult: OrderValidationResult? = null) {

    private constructor(builder: Builder) : this(builder.uid,
            builder.messageId,
            builder.limitOrder,
            builder.isCancelOrders,
            builder.assetPair,
            builder.baseAsset,
            builder.quotingAsset,
            builder.baseAssetDisabled,
            builder.quotingAssetDisabled,
            builder.limitAsset,
            builder.isTrustedClient,
            builder.processedMessage)

    override fun toString(): String {
        val order = this.limitOrder

        return "id: $uid" +
                ", messageId: $messageId" +
                ", type: ${order.type}" +
                ", client: ${order.clientId}" +
                ", isTrustedClient: $isTrustedClient" +
                ", assetPair: ${order.assetPairId}" +
                ", baseAssetDisabled: $baseAssetDisabled" +
                ", quotingAssetDisabled: $quotingAssetDisabled" +
                ", volume: ${NumberUtils.roundForPrint(order.volume)}" +
                ", price: ${NumberUtils.roundForPrint(order.price)}" +
                (if (order.lowerLimitPrice != null) ", lowerLimitPrice: ${NumberUtils.roundForPrint(order.lowerLimitPrice)}" else "") +
                (if (order.lowerPrice != null) ", lowerPrice: ${NumberUtils.roundForPrint(order.lowerPrice)}" else "") +
                (if (order.upperLimitPrice != null) ", upperLimitPrice: ${NumberUtils.roundForPrint(order.upperLimitPrice)}" else "") +
                (if (order.upperPrice != null) ", upperPrice: ${NumberUtils.roundForPrint(order.upperPrice)}" else "") +
                ", cancel: $isCancelOrders" +
                ", fee: ${order.fee}" +
                ", fees: ${order.fees}"
    }

    class Builder {
        var uid: String? = null
        lateinit var messageId: String
        lateinit var limitOrder: LimitOrder
        var processedMessage: ProcessedMessage? = null
        lateinit var assetPair: AssetPair
        lateinit var baseAsset: Asset
        lateinit var quotingAsset: Asset
        lateinit var limitAsset: Asset
        var baseAssetDisabled: Boolean = false
        var quotingAssetDisabled: Boolean = false
        var isTrustedClient: Boolean = false
        var isCancelOrders: Boolean = false

        fun limitOrder(limitOrder: LimitOrder) = apply { this.limitOrder = limitOrder }
        fun cancelOrders(cancelOrders: Boolean) = apply { this.isCancelOrders = cancelOrders }
        fun uid(uid: String?) = apply { this.uid = uid }
        fun messageId(messageId: String) = apply { this.messageId = messageId }
        fun processedMessage(processedMessage: ProcessedMessage?) = apply { this.processedMessage = processedMessage }
        fun assetPair(assetPair: AssetPair) = apply { this.assetPair = assetPair }
        fun baseAsset(asset: Asset) = apply { this.baseAsset = asset }
        fun quotingAsset(asset: Asset) = apply { this.quotingAsset = asset }
        fun trustedClient(trustedClient: Boolean) = apply { this.isTrustedClient = trustedClient }
        fun limitAsset(limitAsset: Asset) = apply { this.limitAsset = limitAsset }
        fun baseAssetDisabled(baseAssetDisabled: Boolean) = apply { this.baseAssetDisabled = baseAssetDisabled }
        fun quotingAssetDisabled(quotingAssetDisabled: Boolean) = apply { this.quotingAssetDisabled = quotingAssetDisabled }

        fun build() = SingleLimitOrderContext(this)
    }
}