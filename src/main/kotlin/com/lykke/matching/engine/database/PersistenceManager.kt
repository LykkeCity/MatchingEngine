package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.common.entity.PersistenceData

interface PersistenceManager {
    fun persist(data: PersistenceData): Boolean
}