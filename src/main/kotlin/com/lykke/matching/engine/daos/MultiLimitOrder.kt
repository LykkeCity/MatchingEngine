package com.lykke.matching.engine.daos

import com.lykke.matching.engine.order.OrderCancelMode

data class MultiLimitOrder(val messageUid: String,
                           val clientId: String,
                           val assetPairId: String,
                           val orders: Collection<NewLimitOrder>,
                           val cancelAllPreviousLimitOrders: Boolean,
                           val cancelBuySide: Boolean,
                           val cancelSellSide: Boolean,
                           val cancelMode: OrderCancelMode,
                           val buyReplacements: Map<String, NewLimitOrder>,
                           val sellReplacements: Map<String, NewLimitOrder>)