package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MidPrice

interface ReadOnlyMidPriceDatabaseAccessor {
    fun all(): List<MidPrice>
}