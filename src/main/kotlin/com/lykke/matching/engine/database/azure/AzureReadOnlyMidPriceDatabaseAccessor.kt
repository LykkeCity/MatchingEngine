package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.ReadOnlyMidPriceDatabaseAccessor

class AzureReadOnlyMidPriceDatabaseAccessor: ReadOnlyMidPriceDatabaseAccessor {
    override fun getAssetPairToMidPrices(): Map<String, List<MidPrice>> {
        return emptyMap()
    }
}