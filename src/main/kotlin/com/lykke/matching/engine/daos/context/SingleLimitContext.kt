package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import java.util.*

class SingleLimitContext(val id: String,
                         val messageId: String,
                         val limitOrder: LimitOrder,
                         val isCancelOrders: Boolean,
                         val orderProcessingStartTime: Date,
                         val assetPair: AssetPair,
                         val baseAsset: Asset,
                         val quotingAsset: Asset,
                         val limitAsset: Asset,
                         val isTrustedClient: Boolean,
                         val processedMessage: ProcessedMessage,
                         var validationResult: OrderValidationResult? = null) {

    private constructor(builder: Builder) : this(builder.id,
            builder.messageId,
            builder.limitOrder,
            builder.isCancelOrders,
            builder.orderProcessingStartTime,
            builder.assetPair,
            builder.baseAsset,
            builder.quotingAsset,
            builder.limitAsset,
            builder.isTrustedClient,
            builder.processedMessage)

    class Builder {
        lateinit var id: String
        lateinit var messageId: String
        lateinit var limitOrder: LimitOrder
        lateinit var orderProcessingStartTime: Date
        lateinit var processedMessage: ProcessedMessage
        lateinit var assetPair: AssetPair
        lateinit var baseAsset: Asset
        lateinit var quotingAsset: Asset
        lateinit var limitAsset: Asset
        var isTrustedClient: Boolean = false
        var isCancelOrders: Boolean = false

        fun limitOrder(limitOrder: LimitOrder) = apply { this.limitOrder = limitOrder }
        fun orderProcessingStartTime(orderProcessingStartTime: Date) = apply { this.orderProcessingStartTime = orderProcessingStartTime }
        fun cancelOrders(cancelOrders: Boolean) = apply { this.isCancelOrders = cancelOrders }
        fun id(id: String) = apply { this.id = id }
        fun messageId(messageId: String) = apply { this.messageId = messageId }
        fun processedMessage(processedMessage: ProcessedMessage) = apply { this.processedMessage = processedMessage }
        fun assetPair(assetPair: AssetPair) = apply { this.assetPair = assetPair }
        fun baseAsset(asset: Asset) = apply { this.baseAsset = asset }
        fun quotingAsset(asset: Asset) = apply { this.quotingAsset = asset }
        fun trustedClient(trustedClient: Boolean) = apply { this.isTrustedClient = trustedClient }
        fun limitAsset(limitAsset: Asset) = apply {this.limitAsset = limitAsset}

        fun build() = SingleLimitContext(this)
    }
}