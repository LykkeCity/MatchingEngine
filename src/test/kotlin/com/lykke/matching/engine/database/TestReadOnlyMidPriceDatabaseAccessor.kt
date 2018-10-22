package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MidPrice

class TestReadOnlyMidPriceDatabaseAccessor: ReadOnlyMidPriceDatabaseAccessor {
    override fun all(): List<MidPrice> {
        return emptyList()
    }
}