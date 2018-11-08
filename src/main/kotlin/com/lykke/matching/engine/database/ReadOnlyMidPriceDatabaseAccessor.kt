package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MidPrice

interface ReadOnlyMidPriceDatabaseAccessor {
    fun getMidPricesByAssetPairMap(): Map<String, List<MidPrice>>
}