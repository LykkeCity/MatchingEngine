package com.lykke.matching.engine.database.common.entity

import com.lykke.matching.engine.daos.NewLimitOrder

class OrderBooksPersistenceData(val orderBooks: Collection<OrderBookPersistenceData>,
                                val ordersToSave: Collection<NewLimitOrder>,
                                val ordersToRemove: Collection<NewLimitOrder>)

class OrderBookPersistenceData(val assetPairId: String,
                               val isBuy: Boolean,
                               val orders: Collection<NewLimitOrder>)

