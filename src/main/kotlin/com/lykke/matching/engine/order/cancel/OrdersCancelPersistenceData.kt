package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData

class OrdersCancelPersistenceData(val limitOrderBooksPersistenceData: OrderBooksPersistenceData,
                                  val stopLimitOrderBooksPersistenceData: OrderBooksPersistenceData)