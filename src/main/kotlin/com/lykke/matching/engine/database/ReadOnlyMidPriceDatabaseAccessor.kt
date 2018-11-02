package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MidPrice

interface ReadOnlyMidPriceDatabaseAccessor {
    fun getAssetPairToMidPrices(): Map<String, List<MidPrice>>
}