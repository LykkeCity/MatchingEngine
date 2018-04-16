package com.lykke.matching.engine.daos

data class MultiLimitOrder(val messageUid: String,
                           val clientId: String,
                           val assetPairId: String,
                           val orders: Collection<NewLimitOrder>,
                           val cancelAllPreviousLimitOrders: Boolean,
                           val cancelBuySide: Boolean,
                           val cancelSellSide: Boolean)