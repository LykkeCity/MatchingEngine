package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.common.PersistenceData

interface PersistenceManager {
    fun persist(data: PersistenceData)
    fun balancesQueueSize(): Int
}