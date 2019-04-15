package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult

data class SingleLimitOrderContext(val messageId: String,
                              val limitOrder: LimitOrder,
                              val isCancelOrders: Boolean,
                              val assetPair: AssetPair?,
                              val baseAsset: Asset?,
                              val quotingAsset: Asset?,
                              val limitAsset: Asset?,
                              val isTrustedClient: Boolean,
                              val processedMessage: ProcessedMessage?,
                              var validationResult: OrderValidationResult? = null) {

    private constructor(builder: Builder) : this(
            builder.messageId,
            builder.limitOrder,
            builder.isCancelOrders,
            builder.assetPair,
            builder.baseAsset,
            builder.quotingAsset,
            builder.limitAsset,
            builder.isTrustedClient,
            builder.processedMessage)

    override fun toString(): String {
        return  ", messageId: $messageId" +
                ", isTrustedClient: $isTrustedClient" +
                ", cancel: $isCancelOrders" +
                ", limitOrder: $limitOrder"
    }

    class Builder {
        lateinit var messageId: String
        lateinit var limitOrder: LimitOrder
        var processedMessage: ProcessedMessage? = null
        var assetPair: AssetPair? = null
        var baseAsset: Asset? = null
        var quotingAsset: Asset? = null
        var limitAsset: Asset? = null
        var isTrustedClient: Boolean = false
        var isCancelOrders: Boolean = false

        fun limitOrder(limitOrder: LimitOrder) = apply { this.limitOrder = limitOrder }
        fun cancelOrders(cancelOrders: Boolean) = apply { this.isCancelOrders = cancelOrders }
        fun messageId(messageId: String) = apply { this.messageId = messageId }
        fun processedMessage(processedMessage: ProcessedMessage?) = apply { this.processedMessage = processedMessage }
        fun assetPair(assetPair: AssetPair?) = apply { this.assetPair = assetPair }
        fun baseAsset(asset: Asset?) = apply { this.baseAsset = asset }
        fun quotingAsset(asset: Asset?) = apply { this.quotingAsset = asset }
        fun trustedClient(trustedClient: Boolean) = apply { this.isTrustedClient = trustedClient }
        fun limitAsset(limitAsset: Asset?) = apply { this.limitAsset = limitAsset }

        fun build() = SingleLimitOrderContext(this)
    }
}