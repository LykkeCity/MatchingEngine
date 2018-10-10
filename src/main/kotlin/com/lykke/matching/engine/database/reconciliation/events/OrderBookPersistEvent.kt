package com.lykke.matching.engine.database.reconciliation.events

import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData

class OrderBookPersistEvent(val persistenceData: Collection<OrderBookPersistenceData>)