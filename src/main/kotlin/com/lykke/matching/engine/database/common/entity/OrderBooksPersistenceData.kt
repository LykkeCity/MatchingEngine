package com.lykke.matching.engine.database.common.entity

import com.lykke.matching.engine.daos.LimitOrder

class OrderBooksPersistenceData(val orderBooks: Collection<OrderBookPersistenceData>,
                                val ordersToSave: Collection<LimitOrder>,
                                val ordersToRemove: Collection<LimitOrder>)

class OrderBookPersistenceData(val assetPairId: String,
                               val isBuy: Boolean,
                               val orders: Collection<LimitOrder>)

