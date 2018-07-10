package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import java.util.*

class SingleLimitContext(val id: String,
                         val messageId: String,
                         val limitOrder: LimitOrder,
                         val cancelOrders: Boolean,
                         val orderProcessingStartTime: Date,
                         val processedMessage: ProcessedMessage) {

    private constructor(builder: Builder) : this(builder.id,
            builder.messageId,
            builder.limitOrder,
            builder.cancelOrders,
            builder.orderProcessingStartTime,
            builder.processedMessage)

    class Builder {
        lateinit var id: String
        lateinit var messageId: String
        lateinit var limitOrder: LimitOrder
        lateinit var orderProcessingStartTime: Date
        lateinit var processedMessage: ProcessedMessage
        var cancelOrders: Boolean = false

        fun limitOrder(limitOrder: LimitOrder) = apply { this.limitOrder = limitOrder }
        fun orderProcessingStartTime(orderProcessingStartTime: Date) = apply { this.orderProcessingStartTime = orderProcessingStartTime }
        fun cancelOrders(cancelOrders: Boolean) = apply { this.cancelOrders = cancelOrders }
        fun id(id: String) = apply { this.id = id }
        fun messageId(messageId: String) = apply { this.messageId = messageId }
        fun processedMessage(processedMessage: ProcessedMessage) = apply {this.processedMessage = processedMessage}

        fun build() = SingleLimitContext(this)
    }
}